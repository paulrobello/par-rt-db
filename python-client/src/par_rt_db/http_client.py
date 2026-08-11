"""One-shot HTTP client for par-rt-db. ``Authorization: Bearer <token>`` on every call.

The sync HTTP surface — the python port of ``rust-client/src/http.rs``'s
``RtDbHttpClient``. Mirrors the rust-client's data plane (``POST /api/query`` /
``POST /api/mutate``), storage (``POST /api/storage/{db}`` raw-body upload,
``DELETE``, ``GET .../metadata``, public serve URL), and admin control plane
(``POST /admin/...``, ``GET /admin/...``, ``POST /admin/db/{db}/...``). Routes,
request bodies, and response shapes are identical to the rust-client; only the
method names are snake_cased to match Python convention.

The reactive WebSocket client (``/sync``) ships as ``par_rt_db.ws_client``
(``pip install par-rt-db[ws]``); this module is sync-only and depends only on
``httpx``.

``httpx`` is an optional dependency (``pip install par-rt-db[http]``); it is
imported lazily inside ``RtDbHttpClient.__init__`` so that importing this module
or ``par_rt_db`` without the ``[http]`` extra does not fail. The error surfaces
only when a caller actually constructs the client without httpx installed.

Token convention (same as rust-client): the bearer passed to the constructor is
sent on every call. For admin methods, construct the client with the instance
admin key as the token; for data-plane methods, construct it with a per-db
machine token (or OAuth session token).

Architecture (ARC-108): the admin methods on this class are thin façades that
delegate to the shared request-description layer in :mod:`par_rt_db.admin` —
each method builds an :class:`~par_rt_db.admin._AdminRequest` via an ``_op_*``
builder and hands it to a :class:`~par_rt_db.admin._SyncAdminExecutor` over this
client's ``httpx.Client``. The admin surface therefore exists in exactly one
place (the builders) rather than being duplicated across the sync/async ×
data-plane/admin clients. New admin surface should be added to the canonical
:class:`par_rt_db.admin.RtDbAdminClient`; the methods here remain for backward
compatibility. The response models are defined once in
:mod:`par_rt_db.admin_models` and re-exported below so every existing
``from par_rt_db.http_client import <Model>`` resolves to the same class object
as ``from par_rt_db import <Model>`` (locked down by
``test_http_client.py::test_top_level_minted_token_is_http_client_minted_token``).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import IO, TYPE_CHECKING, Any, Literal

from pydantic import TypeAdapter

from .admin import (
    _op_admin_mutate,
    _op_admin_query,
    _op_admins_add,
    _op_admins_list,
    _op_admins_remove,
    _op_allowlist_add,
    _op_allowlist_list,
    _op_allowlist_remove,
    _op_backup_now,
    _op_create_db,
    _op_db_stats,
    _op_delete_backup,
    _op_delete_db,
    _op_download_backup,
    _op_export_db,
    _op_get_config,
    _op_get_schema,
    _op_import_db,
    _op_list_backups,
    _op_list_dbs,
    _op_list_tokens,
    _op_metrics,
    _op_migrate_schema,
    _op_mint_token,
    _op_ops_recent,
    _op_patch_config,
    _op_push_schema,
    _op_restore_backup,
    _op_revoke_token,
    _SyncAdminExecutor,
)
from .admin_models import (
    _STEP_RESULT_ADAPTER,
    AdminMember,
    AuditEntry,
    CastFailure,
    ConfigResponse,
    DbStats,
    DbSubCounters,
    DirectiveReport,
    HotConfig,
    HotConfigPatch,
    LatencyStats,
    MetricsSnapshot,
    MigrateResult,
    MintedToken,
    OpEvent,
    SampleChange,
    SchemaHistoryEntry,
    SchemaHistorySummary,
    SessionInfo,
    SubscriptionInfo,
    SubscriptionsPrincipal,
    SubscriptionsResponse,
    TableStat,
    TokenInfo,
    Webhook,
    WebhookDelivery,
    _Wire,
)
from .errors import ErrorCode, RtDbError
from .migration import Directive, MigrateRequest
from .mutation import StepResult, Transaction
from .query import Query, TableQuery, _terminal_of, parse_result
from .schema import SchemaDef
from .wire import BatchQueryOutcome, ScheduleInfo, ScheduleWhen

if TYPE_CHECKING:
    import httpx


# Re-exports: admin response models are canonically defined in
# :mod:`par_rt_db.admin_models` but are re-exported here for backward
# compatibility (existing ``from par_rt_db.http_client import <Model>`` imports
# must resolve to the same class object — see ARC-108). ``__all__`` declares the
# full public surface so linters do not flag these as unused.
__all__ = [
    "RtDbHttpClient",
    # storage models (defined here)
    "UploadResult",
    "FileMetadata",
    "SignedUrl",
    # admin response models (re-exported from .admin_models for backward compat)
    "AdminMember",
    "AuditEntry",
    "CastFailure",
    "ConfigResponse",
    "DbStats",
    "DbSubCounters",
    "DirectiveReport",
    "HotConfig",
    "HotConfigPatch",
    "LatencyStats",
    "MetricsSnapshot",
    "MigrateResult",
    "MintedToken",
    "OpEvent",
    "SampleChange",
    "SchemaHistoryEntry",
    "SchemaHistorySummary",
    "SessionInfo",
    "SubscriptionInfo",
    "SubscriptionsPrincipal",
    "SubscriptionsResponse",
    "TableStat",
    "TokenInfo",
    "Webhook",
    "WebhookDelivery",
]


# --- storage models (data-plane shapes; defined here, not admin shapes) ---


class UploadResult(_Wire):
    """``POST /api/storage/{db}`` response: server-computed file identity.

    ``content_type`` defaults to ``None`` so an older server omitting the field
    still deserializes (mirrors the rust-client's ``#[serde(default)]``).
    """

    id: str
    sha256: str
    size: int
    content_type: str | None = None


class FileMetadata(_Wire):
    """``GET /api/storage/{db}/{id}/metadata`` response: ``UploadResult`` plus
    the server-recorded ``creationTime``."""

    id: str
    sha256: str
    size: int
    content_type: str | None = None
    creation_time: int


class SignedUrl(_Wire):
    """``GET /api/storage/{db}/{id}/signed-url`` response: a time-limited signed
    serve URL and its absolute expiry (epoch milliseconds)."""

    url: str
    expires_at: int


# ``list[...]`` aliases need a TypeAdapter to validate at runtime.
# ``_STEP_RESULT_ADAPTER`` is imported from :mod:`par_rt_db.admin_models` (one
# shared adapter across the data-plane and admin clients).
_SCHEDULES_ADAPTER = TypeAdapter(list[ScheduleInfo])
_BATCH_ADAPTER = TypeAdapter(list[BatchQueryOutcome])


class RtDbHttpClient:
    """Sync one-shot HTTP client. Data plane + storage + admin control plane.

    Construct with a per-db machine token for data-plane/storage calls, or with
    the instance admin key for admin calls (same bearer-everywhere convention as
    the rust-client). Use as a context manager to close the underlying
    ``httpx.Client``::

        with RtDbHttpClient(url, db, token) as c:
            doc = c.get("items", "i1", model=Item)

    Reactive WebSocket (``/sync``) is NOT provided here — it is a separate async
    surface tracked as a follow-on plan.

    The admin methods (``create_db`` / ``push_schema`` / ``mint_token`` / ...)
    are thin façades over the shared admin request layer in
    :mod:`par_rt_db.admin`; the canonical, full-surface admin client is
    :class:`par_rt_db.admin.RtDbAdminClient`. The methods here remain for
    backward compatibility — behavior, signatures, and return types are
    identical.
    """

    def __init__(
        self,
        url: str,
        db: str,
        token: str,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        try:
            import httpx as _httpx
        except ImportError as e:  # pragma: no cover - exercised when [http] absent
            raise ImportError(
                "httpx is required for RtDbHttpClient: install with `pip install par-rt-db[http]`"
            ) from e
        base = url.rstrip("/")
        self._httpx = _httpx
        self._client: httpx.Client = _httpx.Client(
            base_url=base,
            headers={"Authorization": f"Bearer {token}"},
            transport=transport,
        )
        self._base = base
        self._db = db
        self._token = token
        self._admin_executor = _SyncAdminExecutor(self._client)

    # --- lifecycle ---

    def close(self) -> None:
        """Close the underlying ``httpx.Client``."""
        self._client.close()

    def __enter__(self) -> RtDbHttpClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # --- data plane: query (machine token) ---

    def run(self, query: Query | TableQuery, *, model: type = dict) -> Any:
        """``POST /api/query`` → parse the result by the query's terminal.

        ``model`` narrows each document: ``dict`` (default) returns raw dicts,
        a pydantic ``BaseModel`` subclass validates each doc, and ``int``/``str``
        /``float``/``Any`` cover the scalar results of ``count``/``aggregate``/
        ``distinct``. The terminal is inferred from ``query`` (see
        ``_terminal_of``); pass a ``Query`` built with the matching terminal so
        the inferred shape matches what you expect.
        """
        built = query.build() if isinstance(query, TableQuery) else query
        body = {"db": self._db, "query": built.model_dump(by_alias=True, mode="json")}
        resp = self._send("POST", "/api/query", json=body)
        result = resp.json()["result"]
        return parse_result(model, _terminal_of(built), result)

    # ``query`` is the ergonomic alias for ``run`` (matches ts-client naming).
    query = run

    def get(self, table: str, id: str, *, model: type = dict) -> Any:
        """Point read: ``{table, get:id}`` → ``model | None``."""
        return self.run(TableQuery(table).get(id), model=model)

    def find_one_by_index(
        self,
        table: str,
        index: str,
        value: Any,
        *,
        model: type = dict,
    ) -> Any:
        """Single-doc lookup on ``index`` via the ``first`` terminal.

        Returns at most one doc (``None`` if no match); on a unique index this
        is exactly-one semantics, on a non-unique index it picks one
        deterministically. Mirrors ``rust-client``'s ``find_one_by_index``.
        """
        q = TableQuery(table).with_index(index).eq(value).first()
        return self.run(q, model=model)

    # --- data plane: mutate (machine token) ---

    def mutate(
        self,
        txn: Transaction,
        *,
        idempotency_key: str | None = None,
    ) -> list[StepResult]:
        """``POST /api/mutate`` → one ``StepResult`` per step.

        ``idempotency_key`` is omitted from the body when ``None`` (matches the
        server's ``skip_serializing_if`` rule).
        """
        body: dict[str, Any] = {
            "db": self._db,
            "txn": txn.model_dump(by_alias=True, mode="json"),
        }
        if idempotency_key is not None:
            body["idempotencyKey"] = idempotency_key
        resp = self._send("POST", "/api/mutate", json=body)
        results = resp.json()["results"]
        return [_STEP_RESULT_ADAPTER.validate_python(r) for r in results]

    def upsert_by_index(
        self,
        table: str,
        index: str,
        value: Any,
        insert: dict[str, Any],
        patch: dict[str, Any],
    ) -> StepResult:
        """One-step upsert by index-field value.

        Builds a single-step transaction that matches ``value`` on ``index`` —
        match → ``patch``, no match → ``insert`` — and runs it. Returns the
        resulting doc id and ``inserted`` flag (the ``StepResult::Upsert`` shape
        in rust-client). Surfaces ``PRECONDITION_FAILED`` if more than one doc
        matches rather than retrying (not a transient conflict).
        """
        from .mutation import Mutation

        txn = Mutation.builder().upsert(table, index, [value], insert, patch).build()
        results = self.mutate(txn)
        if not results:
            raise RtDbError(ErrorCode.INTERNAL, "upsert returned no result")
        return results[0]

    # --- data plane: scheduling (POST /api/schedule*) ---

    def schedule(self, txn: Transaction, when: ScheduleWhen) -> str:
        """``POST /api/schedule`` → the new schedule's id.

        ``when`` is a ``ScheduleWhen`` (``AfterMs``/``RunAt``/``Cron``, imported
        from the package root — ``from par_rt_db import AfterMs``). One-shot jobs
        past due run immediately; cron jobs skip missed windows (server-side
        semantics).
        """
        body = {
            "db": self._db,
            "when": when.model_dump(by_alias=True, mode="json"),
            "txn": txn.model_dump(by_alias=True, mode="json"),
        }
        resp = self._send("POST", "/api/schedule", json=body)
        return str(resp.json()["id"])

    def cancel_schedule(self, id: str) -> None:
        """``POST /api/schedule/{id}/cancel``."""
        self._manage_schedule(id, "cancel")

    def pause_schedule(self, id: str) -> None:
        """``POST /api/schedule/{id}/pause``."""
        self._manage_schedule(id, "pause")

    def resume_schedule(self, id: str) -> None:
        """``POST /api/schedule/{id}/resume``."""
        self._manage_schedule(id, "resume")

    def _manage_schedule(self, id: str, op: str) -> None:
        """Shared authorize-then-op for cancel/pause/resume (``{db}`` body → ``{ok}``)."""
        resp = self._send("POST", f"/api/schedule/{id}/{op}", json={"db": self._db})
        self._expect_ok(resp)

    def list_schedules(self) -> list[ScheduleInfo]:
        """``POST /api/schedules`` → every schedule for this db."""
        resp = self._send("POST", "/api/schedules", json={"db": self._db})
        return _SCHEDULES_ADAPTER.validate_python(resp.json()["schedules"])

    # --- data plane: batch query (POST /api/query-batch) ---

    def batch_query(self, queries: list[Query | TableQuery]) -> list[BatchQueryOutcome]:
        """Fan out over many queries in one round trip → one outcome per input.

        ``POST /api/query-batch`` runs auth/owner resolution once for the whole
        request; each query's outcome lands in its own aligned slot. An errored
        query becomes that slot's ``{ok: false, error}`` and never fails the
        batch — only the db-level bearer/authorize gate returns non-200. Each
        ``BatchQueryOutcome.result`` is the raw untagged ``QueryResult``; decode
        with ``query.parse_result(model, terminal, outcome.result)`` per query.
        """
        wire_queries = [
            (q.build() if isinstance(q, TableQuery) else q).model_dump(by_alias=True, mode="json")
            for q in queries
        ]
        resp = self._send(
            "POST", "/api/query-batch", json={"db": self._db, "queries": wire_queries}
        )
        return _BATCH_ADAPTER.validate_python(resp.json()["results"])

    # --- storage (machine token; HTTP-only, bypasses the committer) ---

    def upload(
        self,
        data: bytes | IO[bytes] | Iterable[bytes],
        *,
        content_type: str | None = None,
    ) -> UploadResult:
        """``POST /api/storage/{db}`` with a raw body → server-computed metadata.

        ``content_type`` sets the ``Content-Type`` header AND is stored as the
        file's type; when ``None`` the header is left unset (httpx defaults to
        ``application/octet-stream``). Unlike every other method the body is NOT
        JSON.

        ``data`` may be ``bytes`` (buffered), a binary file-like object (anything
        with ``.read()`` — streamed in 64 KiB chunks), or an iterable of bytes
        chunks (streamed, chunked transfer-encoding). httpx handles all three
        natively when passed via ``content=``, so large files are uploaded
        without being fully buffered in memory (ENH-021).
        """
        if isinstance(data, (str, Mapping)) or not isinstance(data, (bytes, Iterable)):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                "upload data must be bytes, a file-like object, or an iterable of bytes",
            )
        headers = {"Content-Type": content_type} if content_type is not None else None
        resp = self._send(
            "POST",
            f"/api/storage/{self._db}",
            content=data,
            headers=headers,
        )
        return UploadResult.model_validate(resp.json())

    def delete_file(self, id: str) -> None:
        """``DELETE /api/storage/{db}/{id}``. Raises ``RtDbError`` on non-2xx."""
        resp = self._send("DELETE", f"/api/storage/{self._db}/{id}")
        if not resp.json().get("ok"):
            raise RtDbError(ErrorCode.INTERNAL, "delete file returned ok=false")

    def get_file_metadata(self, id: str) -> FileMetadata:
        """``GET /api/storage/{db}/{id}/metadata`` → stored file metadata."""
        resp = self._send("GET", f"/api/storage/{self._db}/{id}/metadata")
        return FileMetadata.model_validate(resp.json())

    def get_signed_url(self, id: str, *, ttl_seconds: int | None = None) -> SignedUrl:
        """``GET /api/storage/{db}/{id}/signed-url`` → ``SignedUrl``.

        ``ttl_seconds`` is optional (server default 1h, capped at 7d); when
        ``None`` the request omits the query parameter.
        """
        params = {"ttlSeconds": ttl_seconds} if ttl_seconds is not None else None
        resp = self._send("GET", f"/api/storage/{self._db}/{id}/signed-url", params=params)
        return SignedUrl.model_validate(resp.json())

    def get_url(self, id: str) -> str:
        """The public serve URL (``GET /storage/{id}``) — no request is made."""
        return f"{self._base}/storage/{id}"

    def transform_url(
        self,
        id: str,
        *,
        w: int | None = None,
        h: int | None = None,
        fit: Literal["cover", "contain", "scale-down"] | None = None,
        q: int | None = None,
        format: Literal["jpeg", "png", "auto"] | None = None,
    ) -> str:
        """The public serve URL for ``id`` with image-transform params (ENH-014).

        No request is made. Params appear in the deterministic order
        ``w, h, fit, q, format``; unset params (and ``format="auto"``, the
        server default) are omitted.
        """
        parts: list[str] = []
        if w is not None:
            parts.append(f"w={w}")
        if h is not None:
            parts.append(f"h={h}")
        if fit is not None:
            parts.append(f"fit={fit}")
        if q is not None:
            parts.append(f"q={q}")
        # "auto" is the server default — omit so the URL stays minimal (rust parity).
        if format is not None and format != "auto":
            parts.append(f"format={format}")
        base = f"{self._base}/storage/{id}"
        return f"{base}?{'&'.join(parts)}" if parts else base

    # --- admin control plane (admin key as the token) ---
    #
    # Façades over the shared admin request layer (ARC-108). Each method builds
    # an ``_AdminRequest`` via an ``_op_*`` builder and delegates to the
    # ``_SyncAdminExecutor``. The canonical full-surface client is
    # :class:`par_rt_db.admin.RtDbAdminClient`; see the ``_op_*`` builders there
    # for the authoritative request-construction and response-parsing logic.

    def create_db(self, name: str) -> None:
        """``POST /admin/create-db`` ``{name}`` → ``{ok:true}``."""
        self._admin_executor.run(_op_create_db(name))

    def delete_db(self, name: str, confirm: str) -> None:
        """``POST /admin/delete-db`` ``{name, confirm}`` → ``{ok:true}``.

        The server rejects with ``BAD_REQUEST`` unless ``confirm == name``
        exactly — the typed confirmation guard against accidental deletion.
        """
        self._admin_executor.run(_op_delete_db(name, confirm))

    def push_schema(self, db: str, schema: SchemaDef) -> None:
        """``POST /admin/push-schema`` ``{db, schema}`` → ``{ok:true}``."""
        self._admin_executor.run(_op_push_schema(db, schema))

    def list_dbs(self) -> list[str]:
        """``GET /admin/dbs`` → ``{databases:[...]}``."""
        return self._admin_executor.run(_op_list_dbs())

    def mint_token(
        self,
        db: str,
        name: str,
        *,
        expires_at: int | None = None,
        read_only: bool = False,
        tables: list[str] | None = None,
    ) -> MintedToken:
        """``POST /admin/mint-token`` with capability fields → ``{tokenId, token}``.

        See :meth:`par_rt_db.admin.RtDbAdminClient.mint_token` for body semantics.
        """
        return self._admin_executor.run(
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
        self._admin_executor.run(_op_revoke_token(token_id))

    def export_db(self, db: str) -> str:
        """``GET /admin/export-db?db=<db>`` → the database snapshot as JSONL text."""
        return self._admin_executor.run(_op_export_db(db))

    def import_db(self, db: str, jsonl: str) -> None:
        """``POST /admin/import-db?db=<db>`` with an ``application/x-ndjson`` body."""
        self._admin_executor.run(_op_import_db(db, jsonl))

    def allowlist_add(self, db: str, email: str) -> None:
        """``POST /admin/allowlist`` ``{db, action:"add", email}`` → ``{ok:true}``."""
        self._admin_executor.run(_op_allowlist_add(db, email))

    def allowlist_remove(self, db: str, email: str) -> None:
        """``POST /admin/allowlist`` ``{db, action:"remove", email}`` → ``{ok:true}``."""
        self._admin_executor.run(_op_allowlist_remove(db, email))

    def allowlist_list(self, db: str) -> list[str]:
        """``GET /admin/allowlist?db=<db>`` → ``{emails:[...]}``."""
        return self._admin_executor.run(_op_allowlist_list(db))

    def admins_list(self) -> list[AdminMember]:
        """``GET /admin/admins`` → ``{admins:[{email, githubId?}]}``."""
        return self._admin_executor.run(_op_admins_list())

    def admins_add(self, email: str, github_id: int | None = None) -> None:
        """``POST /admin/admins`` ``{email, githubId?}`` → ``{ok:true}``.

        ``githubId`` is omitted from the body when ``None`` (matches the
        server's ``skip_serializing_if`` rule).
        """
        self._admin_executor.run(_op_admins_add(email, github_id))

    def admins_remove(self, email: str) -> None:
        """``DELETE /admin/admins`` ``{email}`` → ``{ok:true}``.

        Body-on-DELETE (axum reads it from the request body, not the URL) —
        mirrors the rust-client's ``delete_json``.
        """
        self._admin_executor.run(_op_admins_remove(email))

    def list_tokens(self, db: str) -> list[TokenInfo]:
        """``GET /admin/tokens?db=<db>`` → ``{tokens:[{id,name,createdAt,revoked}]}``."""
        return self._admin_executor.run(_op_list_tokens(db))

    def get_schema(self, db: str) -> SchemaDef:
        """``GET /admin/dbs/{db}/schema`` → the database's pushed ``SchemaDef``."""
        return self._admin_executor.run(_op_get_schema(db))

    def db_stats(self, db: str) -> DbStats:
        """``GET /admin/dbs/{db}/stats`` → per-table row counts + storage sizes."""
        return self._admin_executor.run(_op_db_stats(db))

    def metrics(self) -> MetricsSnapshot:
        """``GET /admin/metrics`` → server-wide counters and gauges."""
        return self._admin_executor.run(_op_metrics())

    def get_config(self) -> ConfigResponse:
        """``GET /admin/config`` → redacted running config + build identity + admins."""
        return self._admin_executor.run(_op_get_config())

    def patch_config(self, patch: HotConfigPatch | Mapping[str, Any]) -> ConfigResponse:
        """``PATCH /admin/config`` with a partial hot-config body → updated config.

        See :meth:`par_rt_db.admin.RtDbAdminClient.patch_config` for body semantics.
        """
        return self._admin_executor.run(_op_patch_config(patch))

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
        return self._admin_executor.run(_op_ops_recent(db=db, table=table, n=n))

    # --- admin data access: owner-bypass query/mutate (admin key) ---

    def admin_query(self, db: str, query: Query | TableQuery, *, model: type = dict) -> Any:
        """``POST /admin/db/{db}/query`` ``{query}`` → parsed ``{result}``.

        See :meth:`par_rt_db.admin.RtDbAdminClient.admin_query` for owner-bypass semantics.
        """
        return self._admin_executor.run(_op_admin_query(db, query, model=model))

    def admin_mutate(
        self,
        db: str,
        txn: Transaction,
        *,
        idempotency_key: str | None = None,
    ) -> list[StepResult]:
        """``POST /admin/db/{db}/mutate`` ``{txn, idempotencyKey?}`` → ``{results}``.

        See :meth:`par_rt_db.admin.RtDbAdminClient.admin_mutate` for owner-bypass semantics.
        """
        return self._admin_executor.run(_op_admin_mutate(db, txn, idempotency_key=idempotency_key))

    def migrate_schema(
        self,
        db: str,
        directives: list[Directive] | MigrateRequest,
        *,
        dry_run: bool = False,
    ) -> MigrateResult:
        """``POST /admin/db/{db}/migrate`` ``{directives, dryRun}`` → ``MigrateResult``.

        See :meth:`par_rt_db.admin.RtDbAdminClient.migrate_schema` for body semantics.
        """
        return self._admin_executor.run(_op_migrate_schema(db, directives, dry_run=dry_run))

    # --- admin control plane: managed backups (admin key) ---

    def backup_now(self) -> None:
        """``POST /admin/backup`` → 202; one ``pg_dump`` runs in the background.

        Idempotent trigger guard: a second call while one is running → 409
        ``CONFLICT``. The dump runs outside the committer (``pg_dump`` is a
        read), so no document tables or subscriptions are touched.
        """
        self._admin_executor.run(_op_backup_now())

    def list_backups(self) -> dict[str, Any]:
        """``GET /admin/backups`` → ``{running: bool, backups: [{name, sizeBytes, createdMs}]}``.

        Newest-first. A missing backup dir returns an empty list rather than
        erroring — the endpoint describes what is on disk. ``running`` is the
        in-progress flag for the manual ``POST /admin/backup`` trigger.
        """
        return self._admin_executor.run(_op_list_backups())

    def download_backup(self, name: str) -> bytes:
        """``GET /admin/backups/{name}`` → the dump file's raw bytes.

        The response body is ``application/octet-stream``; do not JSON-decode.
        The server validates ``name`` (``rtdb-<stamp>.dump`` shape) before any
        filesystem access, so a traversal-shaped name is rejected at the edge.
        """
        return self._admin_executor.run(_op_download_backup(name))

    def delete_backup(self, name: str) -> None:
        """``DELETE /admin/backups/{name}`` → 204; removes one dump file.

        Same ``validate_dump_name`` short-circuit as download; 404 if the file
        is already gone.
        """
        self._admin_executor.run(_op_delete_backup(name))

    def restore_backup(self, name: str) -> dict[str, Any]:
        """``POST /admin/restore`` ``{name, confirm}`` → ``{target, instructions}``.

        ``confirm`` is sent equal to ``name`` (typed guard, mirroring
        ``delete_db``). The live DB is never touched: restore creates a fresh
        ``rtdb_restored_<stamp>`` DB and ``pg_restore``s into it. The response
        carries the target DB name and cutover instructions.
        """
        return self._admin_executor.run(_op_restore_backup(name))

    # --- request plumbing ---

    def _send(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Issue a request; raise ``RtDbError`` on non-2xx, else return response.

        The bearer header is set on the underlying ``httpx.Client`` (constructor
        time), so callers pass only the method, path, and per-request kwargs
        (``json``/``content``/``headers``/``params``).
        """
        resp = self._client.request(method, path, **kwargs)
        if not resp.is_success:
            raise RtDbError.from_http(resp.status_code, resp.content)
        return resp

    @staticmethod
    def _expect_ok(resp: httpx.Response) -> None:
        """Assert the body is ``{ok: true}``; raise ``RtDbError`` otherwise."""
        if not resp.json().get("ok"):
            raise RtDbError(ErrorCode.INTERNAL, "admin request returned ok=false")
