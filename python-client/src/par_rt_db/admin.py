"""Admin control-plane client for par-rt-db.

Dedicated admin-key bearer client for the full ``/admin/*`` surface, plus the
transport-agnostic request layer shared with the data-plane HTTP clients
(ARC-108). The admin methods on :class:`par_rt_db.http_client.RtDbHttpClient`
and :class:`par_rt_db.aio_http_client.RtDbAsyncHttpClient` are thin façades over
the same request builders + executors defined here, so the ~40 admin operations
exist in exactly one place rather than four near-identical copies.

Surface covered:

* db lifecycle — ``create-db`` / ``delete-db`` / ``dbs`` / ``clone-db``
* schema — ``push-schema`` / ``dbs/{db}/schema`` / ``db/{db}/schema/preview`` /
  ``dbs/{db}/migrate`` / schema history (list / get-version / restore)
* export / import — ``export-db`` / ``import-db``
* allowlist — ``/admin/allowlist`` (add / remove / list)
* admins — ``/admin/admins`` (list / add / remove)
* introspection — ``dbs/{db}/stats`` / ``metrics`` / ``ops/recent`` /
  ``config`` (get + patch)
* query introspection (ENH-019) — ``db/{db}/explain`` (compiled SQL + params)
  and ``slow-queries`` (the bounded slow-query ring)
* owner-bypass data access — ``/admin/db/{db}/query|mutate``
* managed backups — ``backup`` / ``backups`` / ``restore``
* token management (ENH-005) — ``mint-token`` / ``revoke-token`` / ``tokens``
  with the capability fields (``expiresAt``, ``readOnly``, ``tables``)
* interactive sessions — ``GET/DELETE /admin/sessions`` (list / revoke-one /
  revoke-user)
* user merge — ``POST /admin/merge-users`` (anon→real account merge, typed
  confirm guard)
* webhook management (ENH-003) — ``/admin/db/{db}/webhooks`` (list / create /
  edit / delete) plus ``.../{id}/deliveries`` for the delivery outbox
* audit log (ENH-004) — ``/admin/audit`` durable per-write audit rows, filtered
  by db/table/op/principal/source with limit/offset paging
* live subscription inspector (ENH-010) — ``/admin/subscriptions`` for the live
  subscription table + fan-out counters, optionally filtered by db
* scheduled jobs — ``GET|POST /admin/db/{db}/schedules`` plus
  ``.../{id}/cancel|pause|resume`` (the admin view spans all principals)
* file storage — ``GET|POST /admin/db/{db}/storage`` (raw-byte upload) plus
  ``DELETE .../{id}`` (idempotent)
* per-db anonymous-access toggle (SEC-103) —
  ``GET|PATCH /admin/db/{db}/anonymous-access``

Architecture (ARC-108): each admin operation is a *builder* function returning
an :class:`_AdminRequest` (HTTP method, path, request kwargs, and a response
``parse`` closure). One sync executor (:class:`_SyncAdminExecutor`) and one
async executor (:class:`_AsyncAdminExecutor`) consume those descriptions; the
four public admin-bearing classes are thin façades that construct the builder
result and hand it to their executor. Request construction and response parsing
are therefore shared across sync/async and data-plane/admin — only the actual
HTTP call differs.

This is the canonical admin surface; the same methods also remain on
:class:`par_rt_db.http_client.RtDbHttpClient` /
:class:`par_rt_db.aio_http_client.RtDbAsyncHttpClient` for backward
compatibility. New code should prefer :class:`RtDbAdminClient` /
:class:`AsyncRtDbAdminClient`.

Two classes mirror the sync/async split of the data-plane HTTP client:
:class:`RtDbAdminClient` (sync, :class:`httpx.Client`) and
:class:`AsyncRtDbAdminClient` (async, :class:`httpx.AsyncClient`). ``httpx``
is imported lazily inside ``__init__`` so this module imports without the
``[http]``/``[aio]`` extra installed; the error surfaces only when a caller
actually constructs a client without httpx available.

The on-the-wire keys are camelCase; Python attributes are snake_case. The
response models (``MintedToken``, ``TokenInfo``, ``DbStats``,
``MetricsSnapshot``, ``ConfigResponse``, ``MigrateResult``, ...) are the
canonical pydantic models defined in :mod:`par_rt_db.admin_models` and
re-exported from here, so an ``isinstance`` check against the top-level
``from par_rt_db import MintedToken`` succeeds regardless of which client
produced the value — there is exactly one model type per response shape across
the sync/async data-plane and admin clients.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from .admin_models import (
    _STEP_RESULT_ADAPTER,
    _UNSET,
    AdminMember,
    AuditEntry,
    ConfigResponse,
    DbStats,
    ExplainResult,
    FileMetadata,
    HotConfigPatch,
    MergeReport,
    MetricsSnapshot,
    MigrateResult,
    MintedToken,
    OpEvent,
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
from .errors import ErrorCode, RtDbError
from .migration import Directive, MigrateRequest
from .mutation import StepResult, Transaction
from .query import Query, TableQuery, _terminal_of, parse_result
from .schema import SchemaDef
from .wire import (
    PROTOCOL_VERSION,
    ScheduleInfo,
    ScheduleWhen,
    WorkflowInfo,
    WorkflowInfoFull,
    WorkflowSpec,
    WorkflowStatus,
)

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


# --- request builders (one per admin operation) ---
#
# Each builder captures the operation's request construction (path interpolation,
# conditional body fields, tri-state sentinels, query-param assembly) AND its
# response parse, so the former four copies collapse to one. The sync/async
# façade classes below delegate to these via the executors.


def _op_create_db(name: str) -> _AdminRequest:
    return _AdminRequest("POST", "/admin/create-db", {"json": {"name": name}}, _parse_expect_ok)


def _op_delete_db(name: str, confirm: str) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        "/admin/delete-db",
        {"json": {"name": name, "confirm": confirm}},
        _parse_expect_ok,
    )


def _op_list_dbs() -> _AdminRequest:
    return _AdminRequest("GET", "/admin/dbs", {}, _parse_databases)


def _op_push_schema(db: str, schema: SchemaDef) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        "/admin/push-schema",
        {"json": {"db": db, "schema": schema.model_dump(by_alias=True, mode="json")}},
        _parse_expect_ok,
    )


def _op_preview_schema(db: str, schema: SchemaDef) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        f"/admin/db/{db}/schema/preview",
        {"json": {"schema": schema.model_dump(by_alias=True, mode="json")}},
        _parse_model(SchemaPreviewDiff),
    )


def _op_get_schema(db: str) -> _AdminRequest:
    return _AdminRequest("GET", f"/admin/dbs/{db}/schema", {}, _parse_model(SchemaDef))


def _op_migrate_schema(
    db: str,
    directives: list[Directive] | MigrateRequest,
    *,
    dry_run: bool,
) -> _AdminRequest:
    wire_directives = (
        directives.directives if isinstance(directives, MigrateRequest) else directives
    )
    body = {
        "directives": [d.model_dump(by_alias=True, mode="json") for d in wire_directives],
        "dryRun": dry_run,
    }
    return _AdminRequest(
        "POST", f"/admin/db/{db}/migrate", {"json": body}, _parse_model(MigrateResult)
    )


def _op_list_schema_history(
    db: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> _AdminRequest:
    params: dict[str, int] = {}
    if limit is not None:
        params["limit"] = limit
    if offset is not None:
        params["offset"] = offset
    return _AdminRequest(
        "GET",
        f"/admin/db/{db}/schema/history",
        {"params": params or None},
        _parse_model_list(SchemaHistorySummary, "entries"),
    )


def _op_get_schema_version(db: str, version: int) -> _AdminRequest:
    return _AdminRequest(
        "GET",
        f"/admin/db/{db}/schema/history/{version}",
        {},
        _parse_model(SchemaHistoryEntry),
    )


def _op_restore_schema(db: str, version: int, *, confirm: str) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        f"/admin/db/{db}/schema/restore",
        {"json": {"version": version, "confirm": confirm}},
        _parse_restored_version,
    )


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


def _op_backup_now() -> _AdminRequest:
    return _AdminRequest("POST", "/admin/backup", {"json": {}}, _parse_expect_ok)


def _op_list_backups() -> _AdminRequest:
    return _AdminRequest("GET", "/admin/backups", {}, _parse_json_object)


def _op_download_backup(name: str) -> _AdminRequest:
    return _AdminRequest("GET", f"/admin/backups/{name}", {}, _parse_bytes, raw_body=True)


def _op_delete_backup(name: str) -> _AdminRequest:
    return _AdminRequest("DELETE", f"/admin/backups/{name}", {}, _parse_none)


def _op_restore_backup(name: str) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        "/admin/restore",
        {"json": {"name": name, "confirm": name}},
        _parse_json_object,
    )


# --- workflow surface (FM-29) ---


def _op_admin_list_workflows(
    db: str,
    *,
    status: WorkflowStatus | None = None,
    limit: int | None = None,
) -> _AdminRequest:
    params: dict[str, Any] = {}
    if status is not None:
        params["status"] = status
    if limit is not None:
        params["limit"] = limit
    return _AdminRequest(
        "GET",
        f"/admin/db/{db}/workflows",
        {"params": params},
        _parse_model_list(WorkflowInfo, "workflows"),
    )


def _op_admin_start_workflow(db: str, spec: WorkflowSpec) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        f"/admin/db/{db}/workflows",
        {"json": spec.model_dump(by_alias=True, mode="json")},
        lambda resp: str(resp.json()["id"]),
    )


def _op_admin_get_workflow(db: str, id: str) -> _AdminRequest:
    return _AdminRequest(
        "GET", f"/admin/db/{db}/workflows/{id}", {}, _parse_model(WorkflowInfoFull)
    )


def _op_admin_cancel_workflow(db: str, id: str) -> _AdminRequest:
    return _AdminRequest(
        "POST", f"/admin/db/{db}/workflows/{id}/cancel", {"json": {}}, _parse_ok_bool
    )


def _op_admin_signal_workflow(
    db: str, id: str, name: str, payload: Any | None = None
) -> _AdminRequest:
    body: dict[str, Any] = {"name": name}
    if payload is not None:
        body["payload"] = payload
    return _AdminRequest(
        "POST", f"/admin/db/{db}/workflows/{id}/signal", {"json": body}, _parse_ok_bool
    )


def _op_admin_delete_workflow(db: str, id: str) -> _AdminRequest:
    return _AdminRequest("DELETE", f"/admin/db/{db}/workflows/{id}", {}, _parse_ok_bool)


# --- admin schedule management ---
#
# GET|POST /admin/db/{db}/schedules + POST .../{id}/cancel|pause|resume.
# Mirrors the ts/rust admin clients one-to-one — paths, bodies, and response
# shapes identical; reuses the wire ``ScheduleInfo``/``ScheduleWhen`` and the
# DSL ``Transaction`` types the client already carries. The manage ops take the
# id + op from the path and no body; ``ok:false`` means an unknown or terminal
# id (a no-op, not an error), so they parse through ``_parse_ok_bool``.


def _op_admin_list_schedules(db: str) -> _AdminRequest:
    return _AdminRequest(
        "GET", f"/admin/db/{db}/schedules", {}, _parse_model_list(ScheduleInfo, "schedules")
    )


def _op_admin_create_schedule(db: str, when: ScheduleWhen, txn: Transaction) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        f"/admin/db/{db}/schedules",
        {
            "json": {
                "when": when.model_dump(by_alias=True, mode="json"),
                "txn": txn.model_dump(by_alias=True, mode="json"),
            }
        },
        lambda resp: str(resp.json()["id"]),
    )


def _op_admin_manage_schedule(db: str, id: str, op: str) -> _AdminRequest:
    """Bodyless POST for cancel/pause/resume — ``op`` is only ever one of those
    three literals, each a path segment the server routes on."""
    return _AdminRequest("POST", f"/admin/db/{db}/schedules/{id}/{op}", {}, _parse_ok_bool)


# --- admin file storage ---
#
# GET|POST /admin/db/{db}/storage + DELETE .../{id}. The upload body is the
# file itself (raw bytes, not JSON); ``content_type`` sets the ``Content-Type``
# header and is left unset when ``None`` (the server then stores the blob
# untyped) — the same convention as the data-plane ``RtDbHttpClient.upload``.


def _op_admin_list_files(db: str) -> _AdminRequest:
    return _AdminRequest(
        "GET", f"/admin/db/{db}/storage", {}, _parse_model_list(FileMetadata, "files")
    )


def _op_admin_upload_file(
    db: str,
    data: bytes,
    *,
    content_type: str | None = None,
) -> _AdminRequest:
    headers = {"Content-Type": content_type} if content_type is not None else None
    return _AdminRequest(
        "POST",
        f"/admin/db/{db}/storage",
        {"content": data, "headers": headers},
        lambda resp: str(resp.json()["id"]),
    )


def _op_admin_delete_file(db: str, id: str) -> _AdminRequest:
    return _AdminRequest("DELETE", f"/admin/db/{db}/storage/{id}", {}, _parse_expect_ok)


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


def _op_mint_token(
    db: str,
    name: str,
    *,
    expires_at: int | None = None,
    read_only: bool = False,
    tables: list[str] | None = None,
) -> _AdminRequest:
    body: dict[str, Any] = {"db": db, "name": name, "readOnly": read_only}
    if expires_at is not None:
        body["expiresAt"] = expires_at
    if tables is not None:
        body["tables"] = list(tables)
    return _AdminRequest("POST", "/admin/mint-token", {"json": body}, _parse_model(MintedToken))


def _op_revoke_token(token_id: str) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        "/admin/revoke-token",
        {"json": {"tokenId": token_id}},
        _parse_expect_ok,
    )


def _op_list_tokens(db: str) -> _AdminRequest:
    return _AdminRequest(
        "GET", "/admin/tokens", {"params": {"db": db}}, _parse_model_list(TokenInfo, "tokens")
    )


def _op_list_sessions(
    *,
    user: str | None = None,
    limit: int | None = None,
) -> _AdminRequest:
    params: dict[str, Any] = {}
    if user is not None:
        params["user"] = user
    if limit is not None:
        params["limit"] = limit
    return _AdminRequest(
        "GET", "/admin/sessions", {"params": params}, _parse_model_list(SessionInfo, "sessions")
    )


def _op_revoke_session(token_hash: str) -> _AdminRequest:
    return _AdminRequest("DELETE", f"/admin/sessions/{token_hash}", {}, _parse_expect_ok)


def _op_revoke_user_sessions(user_id: str) -> _AdminRequest:
    return _AdminRequest(
        "DELETE",
        "/admin/sessions",
        {"params": {"user": user_id}},
        _parse_revoked_count,
    )


def _op_revoke_expired_sessions() -> _AdminRequest:
    return _AdminRequest(
        "DELETE",
        "/admin/sessions",
        {"params": {"expired": "true"}},
        _parse_revoked_count,
    )


def _op_merge_users(anon_user_id: str, real_user_id: str) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        "/admin/merge-users",
        {
            "json": {
                "anonUserId": anon_user_id,
                "realUserId": real_user_id,
                "confirm": real_user_id,
            }
        },
        _parse_model(MergeReport),
    )


def _op_list_webhooks(db: str) -> _AdminRequest:
    return _AdminRequest(
        "GET",
        f"/admin/db/{db}/webhooks",
        {},
        _parse_model_list(Webhook, "webhooks"),
    )


def _op_create_webhook(
    db: str,
    *,
    url: str,
    table: str | None = None,
    events: list[str] | None = None,
    enabled: bool | None = None,
) -> _AdminRequest:
    body: dict[str, Any] = {"url": url}
    if table is not None:
        body["table"] = table
    if events is not None:
        body["events"] = list(events)
    if enabled is not None:
        body["enabled"] = enabled
    return _AdminRequest("POST", f"/admin/db/{db}/webhooks", {"json": body}, _parse_webhook_id)


def _op_edit_webhook(
    db: str,
    id: int,
    *,
    url: str | None = None,
    table: str | None | object = _UNSET,
    events: list[str] | None = None,
    enabled: bool | None = None,
    rotate_secret: bool | None = None,
) -> _AdminRequest:
    body: dict[str, Any] = {}
    if url is not None:
        body["url"] = url
    if table is not _UNSET:
        # ``table`` may be ``None`` (clear to all-tables) or a string (set);
        # both are valid body values, so assign verbatim.
        body["table"] = table
    if events is not None:
        body["events"] = list(events)
    if enabled is not None:
        body["enabled"] = enabled
    if rotate_secret is not None:
        body["rotateSecret"] = rotate_secret
    return _AdminRequest(
        "PUT", f"/admin/db/{db}/webhooks/{id}", {"json": body}, _parse_model(Webhook)
    )


def _op_delete_webhook(db: str, id: int) -> _AdminRequest:
    return _AdminRequest(
        "DELETE",
        f"/admin/db/{db}/webhooks/{id}",
        {},
        _parse_expect_ok,
    )


def _op_list_deliveries(
    db: str,
    id: int,
    *,
    status: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> _AdminRequest:
    params: dict[str, Any] = {}
    if status is not None:
        params["status"] = status
    if limit is not None:
        params["limit"] = limit
    if offset is not None:
        params["offset"] = offset
    return _AdminRequest(
        "GET",
        f"/admin/db/{db}/webhooks/{id}/deliveries",
        {"params": params},
        _parse_model_list(WebhookDelivery, "deliveries"),
    )


def _op_get_audit(
    db: str,
    *,
    table: str | None = None,
    op: str | None = None,
    principal: str | None = None,
    source: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> _AdminRequest:
    params: dict[str, Any] = {"db": db}
    if table is not None:
        params["table"] = table
    if op is not None:
        params["op"] = op
    if principal is not None:
        params["principal"] = principal
    if source is not None:
        params["source"] = source
    if limit is not None:
        params["limit"] = limit
    if offset is not None:
        params["offset"] = offset
    return _AdminRequest(
        "GET", "/admin/audit", {"params": params}, _parse_model_list(AuditEntry, "entries")
    )


def _op_get_subscriptions(db: str | None = None) -> _AdminRequest:
    params: dict[str, Any] = {}
    if db is not None:
        params["db"] = db
    return _AdminRequest(
        "GET",
        "/admin/subscriptions",
        {"params": params},
        _parse_model(SubscriptionsResponse),
    )


def _op_explain(db: str, query: Query | TableQuery) -> _AdminRequest:
    built = query.build() if isinstance(query, TableQuery) else query
    body = {"query": built.model_dump(by_alias=True, mode="json")}
    return _AdminRequest(
        "POST", f"/admin/db/{db}/explain", {"json": body}, _parse_model(ExplainResult)
    )


def _op_slow_queries(*, db: str | None = None, limit: int | None = None) -> _AdminRequest:
    params: dict[str, Any] = {}
    if db is not None:
        params["db"] = db
    if limit is not None:
        params["limit"] = limit
    return _AdminRequest(
        "GET", "/admin/slow-queries", {"params": params}, _parse_model(SlowQueriesResponse)
    )


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


# ---------------------------------------------------------------------------
# Public façade classes. Each method is a one-line delegation: build the
# request description, hand it to the executor. The admin surface therefore
# exists once (in the builders above) rather than four times.
# ---------------------------------------------------------------------------


class RtDbAdminClient:
    """Sync admin control-plane client (the ``[http]`` extra).

    Authenticates every call with the instance admin key (bearer). Construct
    with the admin key and use as a context manager to close the underlying
    :class:`httpx.Client`::

        with RtDbAdminClient(url, admin_key) as c:
            minted = c.mint_token("mydb", "scraper", read_only=True, tables=["users"])
            c.push_schema("mydb", schema)
            stats = c.db_stats("mydb")

    Full admin surface: db lifecycle (incl. clone), schema push/get/migrate +
    history/restore, export/import, allowlist + admin-member management,
    introspection (stats/metrics/ops/config + explain/slow-queries), owner-bypass
    query/mutate, managed backups, the ENH-005 token triple, interactive
    sessions, user merges, webhooks, the audit log, and the live subscription
    inspector.
    Routes, bodies, and response models are byte-identical with the server and
    with :class:`par_rt_db.http_client.RtDbHttpClient`'s admin methods (both
    delegate to the same request layer — see :mod:`par_rt_db.admin`).
    """

    def __init__(
        self,
        base_url: str,
        admin_key: str,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        try:
            import httpx as _httpx
        except ImportError as e:  # pragma: no cover - exercised when [http] absent
            raise ImportError(
                "httpx is required for RtDbAdminClient: install with `pip install par-rt-db[http]`"
            ) from e
        self._httpx = _httpx
        self._base = base_url.rstrip("/")
        self._admin_key = admin_key
        self._client: httpx.Client = _httpx.Client(
            base_url=self._base,
            headers={
                "Authorization": f"Bearer {admin_key}",
                # ARC-013: lets the server diagnose/reject a version mismatch
                # instead of a generic 400 from `deny_unknown_fields`.
                "X-Rtdb-Protocol": str(PROTOCOL_VERSION),
            },
            transport=transport,
        )
        self._executor = _SyncAdminExecutor(self._client)

    # --- lifecycle ---

    def close(self) -> None:
        """Close the underlying ``httpx.Client``."""
        self._client.close()

    def __enter__(self) -> RtDbAdminClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # --- db lifecycle ---

    def create_db(self, name: str) -> None:
        """``POST /admin/create-db`` ``{name}`` → ``{ok:true}``."""
        self._executor.run(_op_create_db(name))

    def delete_db(self, name: str, confirm: str) -> None:
        """``POST /admin/delete-db`` ``{name, confirm}`` → ``{ok:true}``.

        The server rejects with ``BAD_REQUEST`` unless ``confirm == name``
        exactly — the typed confirmation guard against accidental deletion.
        """
        self._executor.run(_op_delete_db(name, confirm))

    def list_dbs(self) -> list[str]:
        """``GET /admin/dbs`` → ``{databases:[...]}``."""
        return self._executor.run(_op_list_dbs())

    # --- schema ---

    def push_schema(self, db: str, schema: SchemaDef) -> None:
        """``POST /admin/push-schema`` ``{db, schema}`` → ``{ok:true}``."""
        self._executor.run(_op_push_schema(db, schema))

    def preview_schema(self, db: str, schema: SchemaDef) -> SchemaPreviewDiff:
        """``POST /admin/db/{db}/schema/preview`` ``{schema}`` → ``SchemaPreviewDiff``.

        Pure/advisory — validates the pending schema and diffs it against the
        currently-applied one WITHOUT applying anything: ``added`` lists every
        new table/column/index an additive-only push would create, ``rejected``
        lists every drop or type change the DDL layer would refuse
        (``push_schema`` remains the authoritative gate). Same body shape as
        :meth:`push_schema` minus the ``db`` key (it rides the path).
        """
        return self._executor.run(_op_preview_schema(db, schema))

    def get_schema(self, db: str) -> SchemaDef:
        """``GET /admin/dbs/{db}/schema`` → the database's pushed ``SchemaDef``."""
        return self._executor.run(_op_get_schema(db))

    def migrate_schema(
        self,
        db: str,
        directives: list[Directive] | MigrateRequest,
        *,
        dry_run: bool = False,
    ) -> MigrateResult:
        """``POST /admin/db/{db}/migrate`` ``{directives, dryRun}`` → ``MigrateResult``.

        Apply (when ``dry_run`` is ``False``) or preview (when ``True``) a
        declarative schema migration. The server validates and folds the
        directives transactionally; on ``dry_run`` nothing is committed and the
        returned ``schema`` is the derived preview (``applied: False``).

        Accepts a list of ``Directive`` model instances (from the ``Migration``
        builder's directives or hand-built) or a full :class:`MigrateRequest`.
        When passing a ``MigrateRequest``, the request's own ``dry_run`` flag is
        overridden by the ``dry_run`` keyword argument (mirrors the rust-client's
        signature: directives + a separate ``dry_run`` bool).
        """
        return self._executor.run(_op_migrate_schema(db, directives, dry_run=dry_run))

    # --- schema history (ENH-013) ---

    def list_schema_history(
        self,
        db: str,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[SchemaHistorySummary]:
        """``GET /admin/db/{db}/schema/history`` → newest-first summary list.

        Optional ``limit`` (server default 100, clamped to 1000) and ``offset``
        (default 0) page the results. Each row omits the ``schema`` blob; use
        :meth:`get_schema_version` for a full snapshot.
        """
        return self._executor.run(_op_list_schema_history(db, limit=limit, offset=offset))

    def get_schema_version(self, db: str, version: int) -> SchemaHistoryEntry:
        """``GET /admin/db/{db}/schema/history/{version}`` → one full snapshot,
        including the ``schema`` blob. Raises :class:`RtDbError` (``not_found``)
        if the database or version does not exist.
        """
        return self._executor.run(_op_get_schema_version(db, version))

    def restore_schema(self, db: str, version: int, *, confirm: str) -> int:
        """``POST /admin/db/{db}/schema/restore`` ``{version, confirm}`` → restore
        the live schema shape to a prior snapshot; returns the restored version.

        ``confirm`` must equal the db name (typed guard, mirrors ``delete-db``).
        """
        return self._executor.run(_op_restore_schema(db, version, confirm=confirm))

    # --- export / import ---

    def export_db(self, db: str) -> str:
        """``GET /admin/export-db?db=<db>`` → the database snapshot as JSONL text."""
        return self._executor.run(_op_export_db(db))

    def import_db(self, db: str, jsonl: str) -> None:
        """``POST /admin/import-db?db=<db>`` with an ``application/x-ndjson`` body."""
        self._executor.run(_op_import_db(db, jsonl))

    def clone_db(self, from_: str, to: str) -> None:
        """``POST /admin/clone-db?from=<from>&to=<to>`` → ``{ok:true}``.

        Clones ``from_`` (schema + documents) into a freshly created ``to`` in
        one server-side step. ``to`` must not already exist; scope matches
        ``export_db``/``import_db`` — storage blobs and scheduled transactions
        are not copied.
        """
        self._executor.run(_op_clone_db(from_, to))

    # --- allowlist ---

    def allowlist_add(self, db: str, email: str) -> None:
        """``POST /admin/allowlist`` ``{db, action:"add", email}`` → ``{ok:true}``."""
        self._executor.run(_op_allowlist_add(db, email))

    def allowlist_remove(self, db: str, email: str) -> None:
        """``POST /admin/allowlist`` ``{db, action:"remove", email}`` → ``{ok:true}``."""
        self._executor.run(_op_allowlist_remove(db, email))

    def allowlist_list(self, db: str) -> list[str]:
        """``GET /admin/allowlist?db=<db>`` → ``{emails:[...]}``."""
        return self._executor.run(_op_allowlist_list(db))

    # --- admins ---

    def admins_list(self) -> list[AdminMember]:
        """``GET /admin/admins`` → ``{admins:[{email, githubId?}]}``."""
        return self._executor.run(_op_admins_list())

    def admins_add(self, email: str, github_id: int | None = None) -> None:
        """``POST /admin/admins`` ``{email, githubId?}`` → ``{ok:true}``.

        ``githubId`` is omitted from the body when ``None`` (matches the
        server's ``skip_serializing_if`` rule).
        """
        self._executor.run(_op_admins_add(email, github_id))

    def admins_remove(self, email: str) -> None:
        """``DELETE /admin/admins`` ``{email}`` → ``{ok:true}``.

        Body-on-DELETE (axum reads it from the request body, not the URL) —
        mirrors the rust-client's ``delete_json``.
        """
        self._executor.run(_op_admins_remove(email))

    # --- introspection ---

    def db_stats(self, db: str) -> DbStats:
        """``GET /admin/dbs/{db}/stats`` → per-table row counts + storage sizes."""
        return self._executor.run(_op_db_stats(db))

    def metrics(self) -> MetricsSnapshot:
        """``GET /admin/metrics`` → server-wide counters and gauges."""
        return self._executor.run(_op_metrics())

    def ops_recent(
        self,
        *,
        db: str | None = None,
        table: str | None = None,
        n: int | None = None,
    ) -> list[OpEvent]:
        """``GET /admin/ops/recent`` → recent document-op events, newest-first.

        All filter opts are optional; omitted filters are not sent. ``n`` caps
        the result count (server-side max 500).
        """
        return self._executor.run(_op_ops_recent(db=db, table=table, n=n))

    # --- config ---

    def get_config(self) -> ConfigResponse:
        """``GET /admin/config`` → redacted running config + build identity + admins."""
        return self._executor.run(_op_get_config())

    def patch_config(self, patch: HotConfigPatch | Mapping[str, Any]) -> ConfigResponse:
        """``PATCH /admin/config`` with a partial hot-config body → updated config.

        Each present field fully replaces the prior value; the server validates
        (``sessionTtlDays>=1``, ``maxFileSize`` within bounds, origin shape).
        Accepts a ``HotConfigPatch`` model or a plain ``Mapping`` of wire camelCase
        keys (e.g. ``{"sessionTtlDays": 60}``); ``None``-valued model fields are
        omitted from the body (matches rust-client's ``skip_serializing_if``).
        """
        return self._executor.run(_op_patch_config(patch))

    # --- owner-bypass data access: admin query/mutate ---

    def admin_query(
        self,
        db: str,
        query: Query | TableQuery,
        *,
        model: type = dict,
        include_deleted: bool | None = None,
    ) -> Any:
        """``POST /admin/db/{db}/query`` ``{query, includeDeleted?}`` → parsed
        ``{result}``.

        Owner-bypass: an admin reads documents across every database regardless
        of ``ownerField``. ``db`` rides in the URL (singular ``db``), so the body
        omits it. Result parsing mirrors ``RtDbHttpClient.run``.

        ``include_deleted`` is an internal admin-route parameter, NOT a wire
        ``Query`` field: ``True`` surfaces soft-deleted (FM-33 ``deleted_at``)
        rows so an operator can see them; ``None``/``False`` (the default)
        omits the key entirely so the server's live-rows-only default applies —
        the key is never sent as ``null``.
        """
        return self._executor.run(
            _op_admin_query(db, query, model=model, include_deleted=include_deleted)
        )

    def admin_mutate(
        self,
        db: str,
        txn: Transaction,
        *,
        idempotency_key: str | None = None,
    ) -> list[StepResult]:
        """``POST /admin/db/{db}/mutate`` ``{txn, idempotencyKey?}`` → ``{results}``.

        Owner-bypass: an admin writes documents across every database. ``db``
        rides in the URL, so the body omits it. ``idempotencyKey`` is omitted
        when ``None``.
        """
        return self._executor.run(_op_admin_mutate(db, txn, idempotency_key=idempotency_key))

    # --- managed backups ---

    def backup_now(self) -> None:
        """``POST /admin/backup`` → 202; one ``pg_dump`` runs in the background.

        Idempotent trigger guard: a second call while one is running → 409
        ``CONFLICT``. The dump runs outside the committer (``pg_dump`` is a
        read), so no document tables or subscriptions are touched.
        """
        self._executor.run(_op_backup_now())

    def list_backups(self) -> dict[str, Any]:
        """``GET /admin/backups`` → ``{running: bool, backups: [{name, sizeBytes, createdMs}]}``.

        Newest-first. A missing backup dir returns an empty list rather than
        erroring — the endpoint describes what is on disk. ``running`` is the
        in-progress flag for the manual ``POST /admin/backup`` trigger.
        """
        return self._executor.run(_op_list_backups())

    def download_backup(self, name: str) -> bytes:
        """``GET /admin/backups/{name}`` → the dump file's raw bytes.

        The response body is ``application/octet-stream``; do not JSON-decode.
        The server validates ``name`` (``rtdb-<stamp>.dump`` shape) before any
        filesystem access, so a traversal-shaped name is rejected at the edge.
        """
        return self._executor.run(_op_download_backup(name))

    def delete_backup(self, name: str) -> None:
        """``DELETE /admin/backups/{name}`` → 204; removes one dump file.

        Same ``validate_dump_name`` short-circuit as download; 404 if the file
        is already gone.
        """
        self._executor.run(_op_delete_backup(name))

    def restore_backup(self, name: str) -> dict[str, Any]:
        """``POST /admin/restore`` ``{name, confirm}`` → ``{target, instructions}``.

        ``confirm`` is sent equal to ``name`` (typed guard, mirroring
        ``delete_db``). The live DB is never touched: restore creates a fresh
        ``rtdb_restored_<stamp>`` DB and ``pg_restore``s into it. The response
        carries the target DB name and cutover instructions.
        """
        return self._executor.run(_op_restore_backup(name))

    # --- workflow surface (FM-29) ---

    def admin_list_workflows(
        self,
        db: str,
        *,
        status: WorkflowStatus | None = None,
        limit: int | None = None,
    ) -> list[WorkflowInfo]:
        """``GET /admin/db/{db}/workflows`` → this db's runs, newest first.

        ``status`` filters to a lifecycle state; ``limit`` caps the page.
        Omitted filters are not sent.
        """
        return self._executor.run(_op_admin_list_workflows(db, status=status, limit=limit))

    def admin_start_workflow(self, db: str, spec: WorkflowSpec) -> str:
        """``POST /admin/db/{db}/workflows`` with the spec as the body → the new
        run's id. The run snapshots ``spec`` at insert time."""
        return self._executor.run(_op_admin_start_workflow(db, spec))

    def admin_get_workflow(self, db: str, id: str) -> WorkflowInfoFull:
        """``GET /admin/db/{db}/workflows/{id}`` → the full row including the
        per-step outcome trail."""
        return self._executor.run(_op_admin_get_workflow(db, id))

    def admin_cancel_workflow(self, db: str, id: str) -> bool:
        """``POST .../workflows/{id}/cancel`` → ``True`` when a pending/running
        run flipped to cancelled; ``False`` when missing or already terminal (a
        no-op, not an error)."""
        return self._executor.run(_op_admin_cancel_workflow(db, id))

    def admin_signal_workflow(
        self, db: str, id: str, name: str, payload: Any | None = None
    ) -> bool:
        """``POST .../workflows/{id}/signal`` → ``True`` when the named signal
        was delivered to a run parked at an ``awaitSignal`` step. ``NOT_FOUND``
        for an unknown run; ``CONFLICT`` when the run is not waiting or is
        waiting on a different name."""
        return self._executor.run(_op_admin_signal_workflow(db, id, name, payload))

    def admin_delete_workflow(self, db: str, id: str) -> bool:
        """``DELETE /admin/db/{db}/workflows/{id}`` → ``True`` when the run row
        was removed; ``False`` when it was already gone."""
        return self._executor.run(_op_admin_delete_workflow(db, id))

    # --- admin schedule management (GET|POST /admin/db/{db}/schedules) ---

    def admin_list_schedules(self, db: str) -> list[ScheduleInfo]:
        """``GET /admin/db/{db}/schedules`` → ``{schedules:[...]}``.

        Lists every pending and in-flight scheduled job for the database (the
        admin view spans all principals — scheduled jobs carry no owner).
        """
        return self._executor.run(_op_admin_list_schedules(db))

    def admin_create_schedule(self, db: str, when: ScheduleWhen, txn: Transaction) -> str:
        """``POST /admin/db/{db}/schedules`` ``{when, txn}`` → the new job's id.

        Registers a scheduled job through the admin surface (the same enqueue
        the ``Schedule`` mutation step and the WS ``schedule`` frame use).
        ``when`` selects one-shot (``AfterMs``/``RunAt``) or recurring
        (``Cron``); ``txn`` is the transaction the scheduler executes at the
        due time.
        """
        return self._executor.run(_op_admin_create_schedule(db, when, txn))

    def admin_cancel_schedule(self, db: str, id: str) -> bool:
        """``POST .../schedules/{id}/cancel`` → ``True`` when a pending job was
        cancelled; ``False`` for an unknown or already-fired id (a no-op, not
        an error)."""
        return self._executor.run(_op_admin_manage_schedule(db, id, "cancel"))

    def admin_pause_schedule(self, db: str, id: str) -> bool:
        """``POST .../schedules/{id}/pause`` → ``True`` when a pending job was
        paused; ``False`` for an unknown or non-pausable id (a no-op)."""
        return self._executor.run(_op_admin_manage_schedule(db, id, "pause"))

    def admin_resume_schedule(self, db: str, id: str) -> bool:
        """``POST .../schedules/{id}/resume`` → ``True`` when a paused job was
        resumed; ``False`` for an unknown or non-paused id (a no-op)."""
        return self._executor.run(_op_admin_manage_schedule(db, id, "resume"))

    # --- admin file storage (GET|POST /admin/db/{db}/storage) ---

    def admin_list_files(self, db: str) -> list[FileMetadata]:
        """``GET /admin/db/{db}/storage`` → ``{files:[...]}``.

        Lists every blob the database owns, newest first (the admin view spans
        all principals — admin-uploaded blobs are owner-less, SEC-118).
        """
        return self._executor.run(_op_admin_list_files(db))

    def admin_upload_file(
        self,
        db: str,
        data: bytes,
        *,
        content_type: str | None = None,
    ) -> str:
        """``POST /admin/db/{db}/storage`` with the RAW bytes as the body (not
        JSON) → the new blob's id.

        ``content_type`` sets the ``Content-Type`` header AND is stored as the
        file's type; when ``None`` the header is left unset and the server
        stores the blob untyped (same convention as the data-plane
        ``RtDbHttpClient.upload``). The server enforces the live ``maxFileSize``
        (413). Admin uploads stay owner-less (SEC-118).
        """
        return self._executor.run(_op_admin_upload_file(db, data, content_type=content_type))

    def admin_delete_file(self, db: str, id: str) -> None:
        """``DELETE /admin/db/{db}/storage/{id}`` → ``{ok:true}``.

        Idempotent — the server acks ok even when the blob is already gone.
        Both the per-db blob row and the global ``storage_index`` row are
        removed, so the public serve URL 404s afterward.
        """
        self._executor.run(_op_admin_delete_file(db, id))

    # --- per-db anonymous-access toggle (SEC-103) ---

    def get_anonymous_access(self, db: str) -> bool:
        """``GET /admin/db/{db}/anonymous-access`` → the per-db flag.

        Reports only the per-database opt-in; the instance-wide
        ``RTDB_AUTH_ANONYMOUS_ENABLED`` boot gate is separate and always
        applies on top (both must allow for an anonymous sign-in to succeed).
        """
        return self._executor.run(_op_get_anonymous_access(db))

    def set_anonymous_access(self, db: str, enabled: bool) -> None:
        """``PATCH /admin/db/{db}/anonymous-access`` ``{enabled}`` → ``{ok:true}``.

        Flips the per-database anonymous-access flag; the instance-wide boot
        gate must also be on for anon minting to work. A ``not_found`` error
        means the database is not registered.
        """
        self._executor.run(_op_set_anonymous_access(db, enabled))

    # --- token surface (ENH-005) ---

    def mint_token(
        self,
        db: str,
        name: str,
        *,
        expires_at: int | None = None,
        read_only: bool = False,
        tables: list[str] | None = None,
    ) -> MintedToken:
        """``POST /admin/mint-token`` → :class:`MintedToken`.

        ``expiresAt`` and ``tables`` are omitted from the body when ``None`` so
        the server applies its defaults (no expiry, all tables). ``readOnly`` is
        always sent — the server's ``#[serde(default)]`` treats absent as
        ``false``, so sending it explicitly is harmless and clearer.
        """
        return self._executor.run(
            _op_mint_token(
                db,
                name,
                expires_at=expires_at,
                read_only=read_only,
                tables=tables,
            )
        )

    def revoke_token(self, token_id: str) -> None:
        """``POST /admin/revoke-token`` ``{tokenId}`` → ``{ok:true}``."""
        self._executor.run(_op_revoke_token(token_id))

    def list_tokens(self, db: str) -> list[TokenInfo]:
        """``GET /admin/tokens?db=<db>`` → ``[TokenInfo, ...]``."""
        return self._executor.run(_op_list_tokens(db))

    # --- interactive sessions (GET/DELETE /admin/sessions) ---

    def list_sessions(
        self,
        *,
        user: str | None = None,
        limit: int | None = None,
    ) -> list[SessionInfo]:
        """``GET /admin/sessions?user=&limit=`` → ``[SessionInfo, ...]``, newest-first.

        Both filters are optional; omitted filters are not sent. ``user`` filters
        by user id or email; ``limit`` is clamped server-side to ``[1, 1000]``
        (default 200). Returns the active interactive sessions across the
        instance — ``tokenHash`` is a non-reversible sha256 digest, safe to
        surface and used to target a row for :meth:`revoke_session`.
        """
        return self._executor.run(_op_list_sessions(user=user, limit=limit))

    def revoke_session(self, token_hash: str) -> None:
        """``DELETE /admin/sessions/{tokenHash}`` → ``{ok:true}``.

        Revokes a single session by its non-reversible sha256 digest (the value
        ``SessionInfo.token_hash`` carries).
        """
        self._executor.run(_op_revoke_session(token_hash))

    def revoke_user_sessions(self, user_id: str) -> int:
        """``DELETE /admin/sessions?user={userId}`` → count of sessions dropped.

        Revokes every session for a user; returns the ``revoked`` count from the
        server's ``{ok, revoked}`` response.
        """
        return self._executor.run(_op_revoke_user_sessions(user_id))

    def revoke_expired_sessions(self) -> int:
        """``DELETE /admin/sessions?expired=true`` → count of sessions dropped.

        Revokes every EXPIRED session instance-wide (OAuth/anonymous and
        admin-key login rows alike); returns the ``revoked`` count from the
        server's ``{ok, revoked}`` response.
        """
        return self._executor.run(_op_revoke_expired_sessions())

    # --- user merge (POST /admin/merge-users) ---

    def merge_users(self, anon_user_id: str, real_user_id: str) -> MergeReport:
        """``POST /admin/merge-users`` ``{anonUserId, realUserId, confirm}`` →
        :class:`MergeReport`.

        Runs the anon→real account merge synchronously across every database
        (per-table doc re-stamps, storage blob owner swap, session re-point,
        guarded anon-row delete) and returns the full report. ``confirm`` is
        sent equal to ``real_user_id`` (the typed guard, mirroring
        :meth:`delete_db`); a missing anon row is refused with ``NOT_FOUND``.
        """
        return self._executor.run(_op_merge_users(anon_user_id, real_user_id))

    # --- webhook surface (ENH-003) ---

    def list_webhooks(self, db: str) -> list[Webhook]:
        """``GET /admin/db/{db}/webhooks`` → ``{webhooks:[...]}``.

        Returns an empty list when webhooks are disabled at boot (the server
        permits the table to not exist), and for a db with no webhooks.
        """
        return self._executor.run(_op_list_webhooks(db))

    def create_webhook(
        self,
        db: str,
        *,
        url: str,
        table: str | None = None,
        events: list[str] | None = None,
        enabled: bool | None = None,
    ) -> int:
        """``POST /admin/db/{db}/webhooks`` ``{url, table?, events?, enabled?}``
        → ``{id}``.

        Returns the new webhook id. ``table=None`` (the default) means
        all-tables (the server's ``tbl = None``); ``events=None`` lets the
        server default to ``["*"]`` (all events); ``enabled=None`` lets the
        server default to enabled (the historical behavior). Each ``None``
        field is omitted from the body so the server applies its defaults.
        """
        return self._executor.run(
            _op_create_webhook(db, url=url, table=table, events=events, enabled=enabled)
        )

    def edit_webhook(
        self,
        db: str,
        id: int,
        *,
        url: str | None = None,
        table: str | None | object = _UNSET,
        events: list[str] | None = None,
        enabled: bool | None = None,
        rotate_secret: bool | None = None,
    ) -> Webhook:
        """``PUT /admin/db/{db}/webhooks/{id}`` → the updated :class:`Webhook`.

        Each kwarg is independently optional on the wire — only kwargs the
        caller passes are sent, omitted fields are left unchanged by the
        server. ``table`` is a tri-state kwarg (the load-bearing case):

        * omitted (``_UNSET``) → omitted from the body → table filter unchanged
        * ``None`` → sent as JSON ``null`` → clears to all-tables
        * ``"items"`` → sent as ``"items"`` → set to that table

        ``url``/``events``/``enabled``/``rotate_secret`` use a plain ``None``
        default (their distinguishing value would be the empty string / empty
        list / etc., so a sentinel is unnecessary): ``None`` means "not passed"
        → omitted from the body → unchanged; pass a real value to set it.
        ``rotate_secret=True`` generates a fresh server-side signing secret
        (SEC-115); the secret value itself is never accepted from the client,
        so this is a flag, not a value.
        """
        return self._executor.run(
            _op_edit_webhook(
                db,
                id,
                url=url,
                table=table,
                events=events,
                enabled=enabled,
                rotate_secret=rotate_secret,
            )
        )

    def delete_webhook(self, db: str, id: int) -> None:
        """``DELETE /admin/db/{db}/webhooks/{id}`` → ``{ok:true}``.

        Cascading pending deliveries via the FK. A non-numeric id is a 400 on
        the server; a missing id is a 404 (returns ``ok:false`` → raises).
        """
        self._executor.run(_op_delete_webhook(db, id))

    def list_deliveries(
        self,
        db: str,
        id: int,
        *,
        status: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[WebhookDelivery]:
        """``GET /admin/db/{db}/webhooks/{id}/deliveries?status=&limit=&offset=``
        → ``{deliveries:[...]}``.

        All three filters are optional; omitted filters are not sent (the
        server then ignores no filter, default limit 100, offset 0). Returns
        an empty list when webhooks are disabled at boot or the webhook has no
        deliveries.
        """
        return self._executor.run(
            _op_list_deliveries(db, id, status=status, limit=limit, offset=offset)
        )

    # --- audit log surface (ENH-004) ---

    def get_audit(
        self,
        db: str,
        *,
        table: str | None = None,
        op: str | None = None,
        principal: str | None = None,
        source: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[AuditEntry]:
        """``GET /admin/audit?db=<db>&table=&op=&principal=&source=&limit=&offset=``
        → ``{entries:[...]}``.

        Durable audit-log rows, newest-first. ``table``/``op``/``principal``/
        ``source`` are optional equality filters (combined with AND); an omitted
        filter is not sent and matches all rows server-side. ``limit`` and
        ``offset`` page the result (server defaults: limit 100, offset 0;
        ``limit`` is clamped to [1, 1000]); both are sent only when not ``None``,
        so an explicit ``0`` for either survives the wire.

        Returns an empty list when audit is disabled at boot
        (``!RTDB_AUDIT_LOG_ENABLED``) — the ``rtdb.audit_log`` table may not
        exist, and the server short-circuits to ``{entries:[]}`` rather than
        surfacing stale rows from a previously enabled run.
        """
        return self._executor.run(
            _op_get_audit(
                db,
                table=table,
                op=op,
                principal=principal,
                source=source,
                limit=limit,
                offset=offset,
            )
        )

    def get_subscriptions(self, db: str | None = None) -> SubscriptionsResponse:
        """``GET /admin/subscriptions[?db=<db>]`` → live subscription table + counters.

        Returns one row per active subscription across every database (the live
        view the committer invalidates against), plus the subscription fan-out
        counters (``subsRerunsTotal``/``subsSkips*Total``/``subsMissedPushesTotal``)
        and a per-db counter breakdown. ``db`` is an optional filter; when
        ``None`` the server returns rows for every database. The ``principal`` on
        each row is ``None`` for non-interactive subscribers (machine tokens,
        scheduled jobs, admin bypass).
        """
        return self._executor.run(_op_get_subscriptions(db))

    # --- query introspection (ENH-019) ---

    def explain_query(self, db: str, query: Query | TableQuery) -> ExplainResult:
        """``POST /admin/db/{db}/explain`` ``{query}`` → compiled SQL + params +
        terminal + warnings.

        Owner-bypass is implicit (admin route). The body shape mirrors
        :meth:`admin_query` — ``db`` rides in the URL, the (optionally built)
        :class:`Query` rides as ``{query}``. ``params`` is the bind-parameter
        list (already stringified server-side); ``warnings`` carries any
        over-approximation or redaction notices.
        """
        return self._executor.run(_op_explain(db, query))

    def get_slow_queries(
        self,
        *,
        db: str | None = None,
        limit: int | None = None,
    ) -> SlowQueriesResponse:
        """``GET /admin/slow-queries[?db=&limit=]`` → bounded slow-query ring.

        Each entry's ``params`` is ``None`` when the server redacted them (the
        ring stores the bind list but the server may withhold it for queries
        that touched sensitive fields). ``threshold_ms`` and ``capacity`` echo
        the boot config (``RTDB_SLOW_QUERY_THRESHOLD_MS`` /
        ``RTDB_SLOW_QUERY_RING_CAP``). Both filters are optional; omitted
        filters are not sent.
        """
        return self._executor.run(_op_slow_queries(db=db, limit=limit))


class AsyncRtDbAdminClient:
    """Async twin of :class:`RtDbAdminClient` (the ``[aio]`` extra).

    Every method is ``async def`` and every request is ``await``-ed; wire
    types, body semantics, and behavior are identical to the sync client. Use
    as an async context manager to close the underlying
    :class:`httpx.AsyncClient`::

        async with AsyncRtDbAdminClient(url, admin_key) as c:
            minted = await c.mint_token("mydb", "scraper", read_only=True)
            await c.push_schema("mydb", schema)
            stats = await c.db_stats("mydb")
    """

    def __init__(
        self,
        base_url: str,
        admin_key: str,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        try:
            import httpx
        except ImportError as e:  # pragma: no cover - exercised when [aio] absent
            raise ImportError(
                "httpx is required for AsyncRtDbAdminClient: "
                "install with `pip install par-rt-db[aio]`"
            ) from e
        self._httpx = httpx
        self._base = base_url.rstrip("/")
        self._admin_key = admin_key
        self._client: httpx.AsyncClient = httpx.AsyncClient(
            base_url=self._base,
            headers={
                "Authorization": f"Bearer {admin_key}",
                # ARC-013: lets the server diagnose/reject a version mismatch
                # instead of a generic 400 from `deny_unknown_fields`.
                "X-Rtdb-Protocol": str(PROTOCOL_VERSION),
            },
            transport=transport,
        )
        self._executor = _AsyncAdminExecutor(self._client)

    # --- lifecycle ---

    async def aclose(self) -> None:
        await self._client.aclose()

    async def close(self) -> None:
        await self.aclose()

    async def __aenter__(self) -> AsyncRtDbAdminClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # --- db lifecycle ---

    async def create_db(self, name: str) -> None:
        """``POST /admin/create-db`` ``{name}`` → ``{ok:true}`` (async)."""
        await self._executor.run(_op_create_db(name))

    async def delete_db(self, name: str, confirm: str) -> None:
        """``POST /admin/delete-db`` ``{name, confirm}`` → ``{ok:true}`` (async).

        See :meth:`RtDbAdminClient.delete_db` for the typed-confirm guard.
        """
        await self._executor.run(_op_delete_db(name, confirm))

    async def list_dbs(self) -> list[str]:
        """``GET /admin/dbs`` → ``{databases:[...]}`` (async)."""
        return await self._executor.run(_op_list_dbs())

    # --- schema ---

    async def push_schema(self, db: str, schema: SchemaDef) -> None:
        """``POST /admin/push-schema`` ``{db, schema}`` → ``{ok:true}`` (async)."""
        await self._executor.run(_op_push_schema(db, schema))

    async def preview_schema(self, db: str, schema: SchemaDef) -> SchemaPreviewDiff:
        """``POST /admin/db/{db}/schema/preview`` → ``SchemaPreviewDiff`` (async).

        See :meth:`RtDbAdminClient.preview_schema` for diff semantics.
        """
        return await self._executor.run(_op_preview_schema(db, schema))

    async def get_schema(self, db: str) -> SchemaDef:
        """``GET /admin/dbs/{db}/schema`` → the database's pushed ``SchemaDef`` (async)."""
        return await self._executor.run(_op_get_schema(db))

    async def migrate_schema(
        self,
        db: str,
        directives: list[Directive] | MigrateRequest,
        *,
        dry_run: bool = False,
    ) -> MigrateResult:
        """``POST /admin/db/{db}/migrate`` → ``MigrateResult`` (async).

        See :meth:`RtDbAdminClient.migrate_schema` for body semantics.
        """
        return await self._executor.run(_op_migrate_schema(db, directives, dry_run=dry_run))

    # --- schema history (ENH-013) ---

    async def list_schema_history(
        self,
        db: str,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[SchemaHistorySummary]:
        """``GET /admin/db/{db}/schema/history`` → newest-first summary list (async).

        See :meth:`RtDbAdminClient.list_schema_history` for body semantics.
        """
        return await self._executor.run(_op_list_schema_history(db, limit=limit, offset=offset))

    async def get_schema_version(self, db: str, version: int) -> SchemaHistoryEntry:
        """``GET /admin/db/{db}/schema/history/{version}`` → one full snapshot (async).

        See :meth:`RtDbAdminClient.get_schema_version`.
        """
        return await self._executor.run(_op_get_schema_version(db, version))

    async def restore_schema(self, db: str, version: int, *, confirm: str) -> int:
        """``POST /admin/db/{db}/schema/restore`` → restored version (async).

        See :meth:`RtDbAdminClient.restore_schema`.
        """
        return await self._executor.run(_op_restore_schema(db, version, confirm=confirm))

    # --- export / import ---

    async def export_db(self, db: str) -> str:
        """``GET /admin/export-db?db=<db>`` → the database snapshot as JSONL text (async)."""
        return await self._executor.run(_op_export_db(db))

    async def import_db(self, db: str, jsonl: str) -> None:
        """``POST /admin/import-db?db=<db>`` with an ``application/x-ndjson`` body (async)."""
        await self._executor.run(_op_import_db(db, jsonl))

    async def clone_db(self, from_: str, to: str) -> None:
        """``POST /admin/clone-db?from=<from>&to=<to>`` → ``{ok:true}`` (async).

        See :meth:`RtDbAdminClient.clone_db`.
        """
        await self._executor.run(_op_clone_db(from_, to))

    # --- allowlist ---

    async def allowlist_add(self, db: str, email: str) -> None:
        """``POST /admin/allowlist`` ``{db, action:"add", email}`` → ``{ok:true}`` (async)."""
        await self._executor.run(_op_allowlist_add(db, email))

    async def allowlist_remove(self, db: str, email: str) -> None:
        """``POST /admin/allowlist`` ``{db, action:"remove", email}`` → ``{ok:true}`` (async)."""
        await self._executor.run(_op_allowlist_remove(db, email))

    async def allowlist_list(self, db: str) -> list[str]:
        """``GET /admin/allowlist?db=<db>`` → ``{emails:[...]}`` (async)."""
        return await self._executor.run(_op_allowlist_list(db))

    # --- admins ---

    async def admins_list(self) -> list[AdminMember]:
        """``GET /admin/admins`` → ``{admins:[{email, githubId?}]}`` (async)."""
        return await self._executor.run(_op_admins_list())

    async def admins_add(self, email: str, github_id: int | None = None) -> None:
        """``POST /admin/admins`` ``{email, githubId?}`` → ``{ok:true}`` (async).

        See :meth:`RtDbAdminClient.admins_add` for the ``githubId`` omission rule.
        """
        await self._executor.run(_op_admins_add(email, github_id))

    async def admins_remove(self, email: str) -> None:
        """``DELETE /admin/admins`` ``{email}`` → ``{ok:true}`` (async).

        Body-on-DELETE — mirrors the rust-client's ``delete_json``.
        """
        await self._executor.run(_op_admins_remove(email))

    # --- introspection ---

    async def db_stats(self, db: str) -> DbStats:
        """``GET /admin/dbs/{db}/stats`` → per-table row counts + storage sizes (async)."""
        return await self._executor.run(_op_db_stats(db))

    async def metrics(self) -> MetricsSnapshot:
        """``GET /admin/metrics`` → server-wide counters and gauges (async)."""
        return await self._executor.run(_op_metrics())

    async def ops_recent(
        self,
        *,
        db: str | None = None,
        table: str | None = None,
        n: int | None = None,
    ) -> list[OpEvent]:
        """``GET /admin/ops/recent`` → recent document-op events, newest-first (async).

        See :meth:`RtDbAdminClient.ops_recent` for filter semantics.
        """
        return await self._executor.run(_op_ops_recent(db=db, table=table, n=n))

    # --- config ---

    async def get_config(self) -> ConfigResponse:
        """``GET /admin/config`` → redacted running config + build identity + admins (async)."""
        return await self._executor.run(_op_get_config())

    async def patch_config(self, patch: HotConfigPatch | Mapping[str, Any]) -> ConfigResponse:
        """``PATCH /admin/config`` with a partial hot-config body → updated config (async).

        See :meth:`RtDbAdminClient.patch_config` for body semantics.
        """
        return await self._executor.run(_op_patch_config(patch))

    # --- owner-bypass data access: admin query/mutate ---

    async def admin_query(
        self,
        db: str,
        query: Query | TableQuery,
        *,
        model: type = dict,
        include_deleted: bool | None = None,
    ) -> Any:
        """``POST /admin/db/{db}/query`` ``{query, includeDeleted?}`` → parsed
        ``{result}`` (async).

        See :meth:`RtDbAdminClient.admin_query` for owner-bypass and
        ``include_deleted`` semantics.
        """
        return await self._executor.run(
            _op_admin_query(db, query, model=model, include_deleted=include_deleted)
        )

    async def admin_mutate(
        self,
        db: str,
        txn: Transaction,
        *,
        idempotency_key: str | None = None,
    ) -> list[StepResult]:
        """``POST /admin/db/{db}/mutate`` ``{txn, idempotencyKey?}`` → ``{results}`` (async).

        See :meth:`RtDbAdminClient.admin_mutate` for owner-bypass semantics.
        """
        return await self._executor.run(_op_admin_mutate(db, txn, idempotency_key=idempotency_key))

    # --- managed backups ---

    async def backup_now(self) -> None:
        """``POST /admin/backup`` → 202; one ``pg_dump`` runs in the background (async).

        See :meth:`RtDbAdminClient.backup_now` for the idempotent-guard semantics.
        """
        await self._executor.run(_op_backup_now())

    async def list_backups(self) -> dict[str, Any]:
        """``GET /admin/backups`` → ``{running, backups:[...]}`` (async)."""
        return await self._executor.run(_op_list_backups())

    async def download_backup(self, name: str) -> bytes:
        """``GET /admin/backups/{name}`` → the dump file's raw bytes (async)."""
        return await self._executor.run(_op_download_backup(name))

    async def delete_backup(self, name: str) -> None:
        """``DELETE /admin/backups/{name}`` → 204; removes one dump file (async)."""
        await self._executor.run(_op_delete_backup(name))

    async def restore_backup(self, name: str) -> dict[str, Any]:
        """``POST /admin/restore`` ``{name, confirm}`` → ``{target, instructions}`` (async).

        See :meth:`RtDbAdminClient.restore_backup` for the typed-confirm guard.
        """
        return await self._executor.run(_op_restore_backup(name))

    # --- workflow surface (FM-29) ---

    async def admin_list_workflows(
        self,
        db: str,
        *,
        status: WorkflowStatus | None = None,
        limit: int | None = None,
    ) -> list[WorkflowInfo]:
        """``GET /admin/db/{db}/workflows`` → this db's runs, newest first (async).

        See :meth:`RtDbAdminClient.admin_list_workflows` for filter semantics.
        """
        return await self._executor.run(_op_admin_list_workflows(db, status=status, limit=limit))

    async def admin_start_workflow(self, db: str, spec: WorkflowSpec) -> str:
        """``POST /admin/db/{db}/workflows`` with the spec as the body → the new
        run's id (async)."""
        return await self._executor.run(_op_admin_start_workflow(db, spec))

    async def admin_get_workflow(self, db: str, id: str) -> WorkflowInfoFull:
        """``GET /admin/db/{db}/workflows/{id}`` → the full row with the outcome
        trail (async)."""
        return await self._executor.run(_op_admin_get_workflow(db, id))

    async def admin_cancel_workflow(self, db: str, id: str) -> bool:
        """``POST .../workflows/{id}/cancel`` → cancel outcome bool (async).

        ``False`` is a legitimate no-op (missing/terminal run), not an error.
        """
        return await self._executor.run(_op_admin_cancel_workflow(db, id))

    async def admin_signal_workflow(
        self, db: str, id: str, name: str, payload: Any | None = None
    ) -> bool:
        """``POST .../workflows/{id}/signal`` → signal delivery outcome bool
        (async).

        ``True`` when delivered. ``NOT_FOUND`` for an unknown run;
        ``CONFLICT`` when the run is not waiting or is waiting on a different
        name.
        """
        return await self._executor.run(_op_admin_signal_workflow(db, id, name, payload))

    async def admin_delete_workflow(self, db: str, id: str) -> bool:
        """``DELETE /admin/db/{db}/workflows/{id}`` → delete outcome bool (async)."""
        return await self._executor.run(_op_admin_delete_workflow(db, id))

    # --- admin schedule management (GET|POST /admin/db/{db}/schedules) ---

    async def admin_list_schedules(self, db: str) -> list[ScheduleInfo]:
        """``GET /admin/db/{db}/schedules`` → ``{schedules:[...]}`` (async).

        See :meth:`RtDbAdminClient.admin_list_schedules` for semantics.
        """
        return await self._executor.run(_op_admin_list_schedules(db))

    async def admin_create_schedule(self, db: str, when: ScheduleWhen, txn: Transaction) -> str:
        """``POST /admin/db/{db}/schedules`` ``{when, txn}`` → the new id (async).

        See :meth:`RtDbAdminClient.admin_create_schedule` for body semantics.
        """
        return await self._executor.run(_op_admin_create_schedule(db, when, txn))

    async def admin_cancel_schedule(self, db: str, id: str) -> bool:
        """``POST .../schedules/{id}/cancel`` → cancel outcome bool (async).

        ``False`` is a legitimate no-op (unknown/fired id), not an error.
        """
        return await self._executor.run(_op_admin_manage_schedule(db, id, "cancel"))

    async def admin_pause_schedule(self, db: str, id: str) -> bool:
        """``POST .../schedules/{id}/pause`` → pause outcome bool (async).

        ``False`` is a legitimate no-op (unknown/non-pausable id), not an error.
        """
        return await self._executor.run(_op_admin_manage_schedule(db, id, "pause"))

    async def admin_resume_schedule(self, db: str, id: str) -> bool:
        """``POST .../schedules/{id}/resume`` → resume outcome bool (async).

        ``False`` is a legitimate no-op (unknown/non-paused id), not an error.
        """
        return await self._executor.run(_op_admin_manage_schedule(db, id, "resume"))

    # --- admin file storage (GET|POST /admin/db/{db}/storage) ---

    async def admin_list_files(self, db: str) -> list[FileMetadata]:
        """``GET /admin/db/{db}/storage`` → ``{files:[...]}`` (async).

        See :meth:`RtDbAdminClient.admin_list_files` for semantics.
        """
        return await self._executor.run(_op_admin_list_files(db))

    async def admin_upload_file(
        self,
        db: str,
        data: bytes,
        *,
        content_type: str | None = None,
    ) -> str:
        """``POST /admin/db/{db}/storage`` with raw bytes → the new blob id (async).

        See :meth:`RtDbAdminClient.admin_upload_file` for the raw-body and
        ``content_type`` semantics.
        """
        return await self._executor.run(_op_admin_upload_file(db, data, content_type=content_type))

    async def admin_delete_file(self, db: str, id: str) -> None:
        """``DELETE /admin/db/{db}/storage/{id}`` → ``{ok:true}`` (async).

        See :meth:`RtDbAdminClient.admin_delete_file` for idempotency semantics.
        """
        await self._executor.run(_op_admin_delete_file(db, id))

    # --- per-db anonymous-access toggle (SEC-103) ---

    async def get_anonymous_access(self, db: str) -> bool:
        """``GET /admin/db/{db}/anonymous-access`` → the per-db flag (async).

        See :meth:`RtDbAdminClient.get_anonymous_access` for the two-gate rule.
        """
        return await self._executor.run(_op_get_anonymous_access(db))

    async def set_anonymous_access(self, db: str, enabled: bool) -> None:
        """``PATCH /admin/db/{db}/anonymous-access`` ``{enabled}`` (async).

        See :meth:`RtDbAdminClient.set_anonymous_access` for semantics.
        """
        await self._executor.run(_op_set_anonymous_access(db, enabled))

    # --- token surface (ENH-005) ---

    async def mint_token(
        self,
        db: str,
        name: str,
        *,
        expires_at: int | None = None,
        read_only: bool = False,
        tables: list[str] | None = None,
    ) -> MintedToken:
        """``POST /admin/mint-token`` → :class:`MintedToken` (async).

        See :meth:`RtDbAdminClient.mint_token` for body semantics.
        """
        return await self._executor.run(
            _op_mint_token(
                db,
                name,
                expires_at=expires_at,
                read_only=read_only,
                tables=tables,
            )
        )

    async def revoke_token(self, token_id: str) -> None:
        """``POST /admin/revoke-token`` ``{tokenId}`` → ``{ok:true}`` (async)."""
        await self._executor.run(_op_revoke_token(token_id))

    async def list_tokens(self, db: str) -> list[TokenInfo]:
        """``GET /admin/tokens?db=<db>`` → ``[TokenInfo, ...]`` (async)."""
        return await self._executor.run(_op_list_tokens(db))

    # --- interactive sessions (GET/DELETE /admin/sessions) ---

    async def list_sessions(
        self,
        *,
        user: str | None = None,
        limit: int | None = None,
    ) -> list[SessionInfo]:
        """``GET /admin/sessions?user=&limit=`` → ``[SessionInfo, ...]`` (async).

        See :meth:`RtDbAdminClient.list_sessions` for filter semantics.
        """
        return await self._executor.run(_op_list_sessions(user=user, limit=limit))

    async def revoke_session(self, token_hash: str) -> None:
        """``DELETE /admin/sessions/{tokenHash}`` → ``{ok:true}`` (async).

        See :meth:`RtDbAdminClient.revoke_session`.
        """
        await self._executor.run(_op_revoke_session(token_hash))

    async def revoke_user_sessions(self, user_id: str) -> int:
        """``DELETE /admin/sessions?user={userId}`` → count of sessions dropped (async).

        See :meth:`RtDbAdminClient.revoke_user_sessions`.
        """
        return await self._executor.run(_op_revoke_user_sessions(user_id))

    async def revoke_expired_sessions(self) -> int:
        """``DELETE /admin/sessions?expired=true`` → count of sessions dropped (async).

        See :meth:`RtDbAdminClient.revoke_expired_sessions`.
        """
        return await self._executor.run(_op_revoke_expired_sessions())

    # --- user merge (POST /admin/merge-users) ---

    async def merge_users(self, anon_user_id: str, real_user_id: str) -> MergeReport:
        """``POST /admin/merge-users`` → :class:`MergeReport` (async).

        See :meth:`RtDbAdminClient.merge_users` for the typed-confirm guard
        and report semantics.
        """
        return await self._executor.run(_op_merge_users(anon_user_id, real_user_id))

    # --- webhook surface (ENH-003) ---

    async def list_webhooks(self, db: str) -> list[Webhook]:
        """``GET /admin/db/{db}/webhooks`` → ``{webhooks:[...]}`` (async).

        See :meth:`RtDbAdminClient.list_webhooks` for empty-list semantics.
        """
        return await self._executor.run(_op_list_webhooks(db))

    async def create_webhook(
        self,
        db: str,
        *,
        url: str,
        table: str | None = None,
        events: list[str] | None = None,
        enabled: bool | None = None,
    ) -> int:
        """``POST /admin/db/{db}/webhooks`` → ``{id}`` (async).

        See :meth:`RtDbAdminClient.create_webhook` for body semantics.
        """
        return await self._executor.run(
            _op_create_webhook(db, url=url, table=table, events=events, enabled=enabled)
        )

    async def edit_webhook(
        self,
        db: str,
        id: int,
        *,
        url: str | None = None,
        table: str | None | object = _UNSET,
        events: list[str] | None = None,
        enabled: bool | None = None,
        rotate_secret: bool | None = None,
    ) -> Webhook:
        """``PUT /admin/db/{db}/webhooks/{id}`` → updated :class:`Webhook` (async).

        See :meth:`RtDbAdminClient.edit_webhook` for the ``table`` tri-state
        (omitted vs ``None`` vs string) and body-building rules, and for the
        ``rotate_secret`` flag (SEC-115).
        """
        return await self._executor.run(
            _op_edit_webhook(
                db,
                id,
                url=url,
                table=table,
                events=events,
                enabled=enabled,
                rotate_secret=rotate_secret,
            )
        )

    async def delete_webhook(self, db: str, id: int) -> None:
        """``DELETE /admin/db/{db}/webhooks/{id}`` → ``{ok:true}`` (async)."""
        await self._executor.run(_op_delete_webhook(db, id))

    async def list_deliveries(
        self,
        db: str,
        id: int,
        *,
        status: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[WebhookDelivery]:
        """``GET /admin/db/{db}/webhooks/{id}/deliveries?status=&limit=&offset=``
        → ``{deliveries:[...]}`` (async).

        See :meth:`RtDbAdminClient.list_deliveries` for filter semantics.
        """
        return await self._executor.run(
            _op_list_deliveries(db, id, status=status, limit=limit, offset=offset)
        )

    # --- audit log surface (ENH-004) ---

    async def get_audit(
        self,
        db: str,
        *,
        table: str | None = None,
        op: str | None = None,
        principal: str | None = None,
        source: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[AuditEntry]:
        """``GET /admin/audit?db=<db>&table=&op=&principal=&source=&limit=&offset=``
        → ``{entries:[...]}`` (async).

        See :meth:`RtDbAdminClient.get_audit` for filter and paging semantics.
        """
        return await self._executor.run(
            _op_get_audit(
                db,
                table=table,
                op=op,
                principal=principal,
                source=source,
                limit=limit,
                offset=offset,
            )
        )

    async def get_subscriptions(self, db: str | None = None) -> SubscriptionsResponse:
        """``GET /admin/subscriptions[?db=<db>]`` → live subscription table + counters (async).

        See :meth:`RtDbAdminClient.get_subscriptions` for response semantics.
        """
        return await self._executor.run(_op_get_subscriptions(db))

    # --- query introspection (ENH-019) ---

    async def explain_query(self, db: str, query: Query | TableQuery) -> ExplainResult:
        """``POST /admin/db/{db}/explain`` → compiled SQL + params + warnings (async).

        See :meth:`RtDbAdminClient.explain_query` for body semantics.
        """
        return await self._executor.run(_op_explain(db, query))

    async def get_slow_queries(
        self,
        *,
        db: str | None = None,
        limit: int | None = None,
    ) -> SlowQueriesResponse:
        """``GET /admin/slow-queries[?db=&limit=]`` → bounded slow-query ring (async).

        See :meth:`RtDbAdminClient.get_slow_queries` for filter and redaction
        semantics.
        """
        return await self._executor.run(_op_slow_queries(db=db, limit=limit))
