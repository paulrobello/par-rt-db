"""Async HTTP/admin/storage client for par-rt-db (the ``[aio]`` extra).

A one-to-one async mirror of :class:`par_rt_db.http_client.RtDbHttpClient` over
:class:`httpx.AsyncClient`: every public method is ``async def`` and every
request is ``await``-ed. Wire types, DSL builders, and the response models are
re-imported from :mod:`par_rt_db.http_client` and the shared modules — nothing
is redefined. ``httpx`` is imported lazily inside ``__init__`` so this module
imports without the ``[aio]`` extra installed.

Architecture (ARC-108): the admin methods on this class are async façades that
delegate to the shared request-description layer in :mod:`par_rt_db.admin` —
each method builds an :class:`~par_rt_db.admin._AdminRequest` via an ``_op_*``
builder and hands it to a :class:`~par_rt_db.admin._AsyncAdminExecutor` over
this client's ``httpx.AsyncClient``. The admin surface therefore exists in
exactly one place (the builders) rather than being duplicated across the
sync/async × data-plane/admin clients. ``[aio]`` is kept as an alias for
``[http]`` — both install ``httpx``, the only dependency this module needs.
"""

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Iterable, Mapping
from typing import IO, TYPE_CHECKING, Any, Literal

from .admin import (
    _AsyncAdminExecutor,
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
)
from .errors import ErrorCode, RtDbError
from .migration import Directive, MigrateRequest
from .mutation import StepResult, Transaction
from .query import Query, TableQuery, _terminal_of, parse_result
from .schema import SchemaDef

if TYPE_CHECKING:
    import httpx

# Re-import the shared response models + adapters this module references.
# The admin models are canonically defined in :mod:`par_rt_db.admin_models` and
# re-exported via :mod:`par_rt_db.http_client`; the storage models
# (``UploadResult``/``FileMetadata``/``SignedUrl``) are defined in
# :mod:`par_rt_db.http_client`. All resolve to the same class objects.
from .http_client import (
    _BATCH_ADAPTER,
    _SCHEDULES_ADAPTER,
    _STEP_RESULT_ADAPTER,
    _WORKFLOWS_ADAPTER,
    AdminMember,
    ConfigResponse,
    DbStats,
    FileMetadata,
    HotConfigPatch,
    MetricsSnapshot,
    MigrateResult,
    MintedToken,
    OpEvent,
    SignedUrl,
    TokenInfo,
    UploadResult,
)
from .wire import (
    BatchQueryOutcome,
    ScheduleInfo,
    ScheduleWhen,
    WorkflowInfo,
    WorkflowSpec,
    WorkflowStatus,
)

# 64 KiB — matches httpx's ``IteratorByteStream.CHUNK_SIZE`` so the async
# adaptation of a sync file-like object chunks at the same granularity.
_ASYNC_UPLOAD_CHUNK = 65_536


async def _async_iter_from_sync(stream: IO[bytes] | Iterable[bytes]) -> AsyncIterator[bytes]:
    """Adapt a sync file-like / iterable of bytes into an async byte iterator.

    ``httpx.AsyncClient`` rejects ``SyncByteStream`` bodies, so a sync
    ``IO[bytes]`` or ``Iterable[bytes]`` passed to :meth:`upload` is read here
    chunk-by-chunk (64 KiB for file-likes; per-item for iterables) and yielded
    on the async path, letting httpx stream it without buffering it whole.
    """
    read = getattr(stream, "read", None)
    if read is not None:
        while True:
            chunk = read(_ASYNC_UPLOAD_CHUNK)
            if not chunk:
                break
            yield chunk
    else:
        for chunk in stream:
            yield chunk


class RtDbAsyncHttpClient:
    """Async twin of :class:`RtDbHttpClient`. See module docstring."""

    def __init__(
        self,
        url: str,
        db: str,
        token: str,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        try:
            import httpx
        except ImportError as e:  # pragma: no cover
            raise ImportError(
                "httpx is required for RtDbAsyncHttpClient: "
                "install with `pip install par-rt-db[aio]`"
            ) from e
        self._httpx = httpx
        self._base = url.rstrip("/")
        self._db = db
        self._token = token
        self._client = httpx.AsyncClient(
            base_url=self._base,
            headers={"Authorization": f"Bearer {token}"},
            transport=transport,
        )
        self._admin_executor = _AsyncAdminExecutor(self._client)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def close(self) -> None:
        await self.aclose()

    async def __aenter__(self) -> RtDbAsyncHttpClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # --- data plane: query (machine token) ---

    async def run(self, query: Query | TableQuery, *, model: type = dict) -> Any:
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
        resp = await self._send("POST", "/api/query", json=body)
        result = resp.json()["result"]
        return parse_result(model, _terminal_of(built), result)

    # ``query`` is the ergonomic alias for ``run`` (matches ts-client naming).
    query = run

    async def get(self, table: str, id: str, *, model: type = dict) -> Any:
        """Point read: ``{table, get:id}`` → ``model | None``."""
        return await self.run(TableQuery(table).get(id), model=model)

    async def find_one_by_index(
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
        return await self.run(q, model=model)

    # --- data plane: mutate (machine token) ---

    async def mutate(
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
        resp = await self._send("POST", "/api/mutate", json=body)
        results = resp.json()["results"]
        return [_STEP_RESULT_ADAPTER.validate_python(r) for r in results]

    async def upsert_by_index(
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
        results = await self.mutate(txn)
        if not results:
            raise RtDbError(ErrorCode.INTERNAL, "upsert returned no result")
        return results[0]

    # --- data plane: scheduling (POST /api/schedule*) ---

    async def schedule(self, txn: Transaction, when: ScheduleWhen) -> str:
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
        resp = await self._send("POST", "/api/schedule", json=body)
        return str(resp.json()["id"])

    async def cancel_schedule(self, id: str) -> None:
        """``POST /api/schedule/{id}/cancel``."""
        await self._manage_schedule(id, "cancel")

    async def pause_schedule(self, id: str) -> None:
        """``POST /api/schedule/{id}/pause``."""
        await self._manage_schedule(id, "pause")

    async def resume_schedule(self, id: str) -> None:
        """``POST /api/schedule/{id}/resume``."""
        await self._manage_schedule(id, "resume")

    async def _manage_schedule(self, id: str, op: str) -> None:
        """Shared authorize-then-op for cancel/pause/resume (``{db}`` body → ``{ok}``)."""
        resp = await self._send("POST", f"/api/schedule/{id}/{op}", json={"db": self._db})
        self._expect_ok(resp)

    async def list_schedules(self) -> list[ScheduleInfo]:
        """``POST /api/schedules`` → every schedule for this db."""
        resp = await self._send("POST", "/api/schedules", json={"db": self._db})
        return _SCHEDULES_ADAPTER.validate_python(resp.json()["schedules"])

    # --- data plane: workflows (POST /api/workflows*) — FM-29 ---

    async def start_workflow(self, spec: WorkflowSpec) -> str:
        """``POST /api/workflows`` → the new run's id."""
        body = {"db": self._db, "spec": spec.model_dump(by_alias=True, mode="json")}
        resp = await self._send("POST", "/api/workflows", json=body)
        return str(resp.json()["id"])

    async def list_workflows(self, status: WorkflowStatus | None = None) -> list[WorkflowInfo]:
        """``POST /api/workflows/list`` → this db's runs, newest first (an
        optional ``status`` filter; capped at 100 server-side)."""
        body: dict[str, Any] = {"db": self._db}
        if status is not None:
            body["status"] = status
        resp = await self._send("POST", "/api/workflows/list", json=body)
        return _WORKFLOWS_ADAPTER.validate_python(resp.json()["workflows"])

    async def cancel_workflow(self, id: str) -> bool:
        """``POST /api/workflows/{id}/cancel`` → ``True`` when a pending/running
        run flipped to cancelled; ``False`` for a missing or already-terminal
        run (a no-op, not an error)."""
        resp = await self._send("POST", f"/api/workflows/{id}/cancel", json={"db": self._db})
        return bool(resp.json()["cancelled"])

    # --- data plane: batch query (POST /api/query-batch) ---

    async def batch_query(self, queries: list[Query | TableQuery]) -> list[BatchQueryOutcome]:
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
        resp = await self._send(
            "POST", "/api/query-batch", json={"db": self._db, "queries": wire_queries}
        )
        return _BATCH_ADAPTER.validate_python(resp.json()["results"])

    # --- storage (machine token; HTTP-only, bypasses the committer) ---

    async def upload(
        self,
        data: bytes | IO[bytes] | Iterable[bytes] | AsyncIterable[bytes],
        *,
        content_type: str | None = None,
    ) -> UploadResult:
        """``POST /api/storage/{db}`` with a raw body → server-computed metadata.

        ``content_type`` sets the ``Content-Type`` header AND is stored as the
        file's type; when ``None`` the header is left unset (httpx defaults to
        ``application/octet-stream``). Unlike every other method the body is NOT
        JSON.

        ``data`` may be ``bytes`` (buffered), a binary file-like object (anything
        with ``.read()``), an iterable of bytes chunks, or an async iterable of
        bytes chunks. ``bytes`` and ``AsyncIterable`` are handed to httpx
        directly; a sync file-like or sync iterable is adapted into an async
        generator so httpx's ``AsyncClient`` streams it without buffering it
        whole. Large files are therefore uploaded without being fully held in
        memory (ENH-021).
        """
        if isinstance(data, (str, Mapping)) or not isinstance(
            data, (bytes, Iterable, AsyncIterable)
        ):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                "upload data must be bytes, a file-like object, or an (async) iterable of bytes",
            )
        # ``bytes`` and ``AsyncIterable`` go straight to httpx (ByteStream is
        # already async-safe; AsyncIterable is httpx's native async path). A
        # sync ``IO[bytes]``/``Iterable[bytes]`` would produce a SyncByteStream,
        # which AsyncClient rejects — wrap it in an async generator.
        if isinstance(data, (bytes, AsyncIterable)):
            body: bytes | AsyncIterable[bytes] = data
        else:
            body = _async_iter_from_sync(data)
        headers = {"Content-Type": content_type} if content_type is not None else None
        resp = await self._send(
            "POST",
            f"/api/storage/{self._db}",
            content=body,
            headers=headers,
        )
        return UploadResult.model_validate(resp.json())

    async def delete_file(self, id: str) -> None:
        """``DELETE /api/storage/{db}/{id}``. Raises ``RtDbError`` on non-2xx."""
        resp = await self._send("DELETE", f"/api/storage/{self._db}/{id}")
        if not resp.json().get("ok"):
            raise RtDbError(ErrorCode.INTERNAL, "delete file returned ok=false")

    async def get_file_metadata(self, id: str) -> FileMetadata:
        """``GET /api/storage/{db}/{id}/metadata`` → stored file metadata."""
        resp = await self._send("GET", f"/api/storage/{self._db}/{id}/metadata")
        return FileMetadata.model_validate(resp.json())

    async def get_signed_url(self, id: str, *, ttl_seconds: int | None = None) -> SignedUrl:
        """``GET /api/storage/{db}/{id}/signed-url`` → ``SignedUrl`` (async)."""
        params = {"ttlSeconds": ttl_seconds} if ttl_seconds is not None else None
        resp = await self._send("GET", f"/api/storage/{self._db}/{id}/signed-url", params=params)
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
    # Async façades over the shared admin request layer (ARC-108). Each method
    # builds an ``_AdminRequest`` via an ``_op_*`` builder and delegates to the
    # ``_AsyncAdminExecutor``. The canonical full-surface client is
    # :class:`par_rt_db.admin.AsyncRtDbAdminClient`; see the ``_op_*`` builders
    # there for the authoritative request-construction and response-parsing logic.

    async def create_db(self, name: str) -> None:
        """``POST /admin/create-db`` ``{name}`` → ``{ok:true}`` (async)."""
        await self._admin_executor.run(_op_create_db(name))

    async def delete_db(self, name: str, confirm: str) -> None:
        """``POST /admin/delete-db`` ``{name, confirm}`` → ``{ok:true}`` (async).

        The server rejects with ``BAD_REQUEST`` unless ``confirm == name``
        exactly — the typed confirmation guard against accidental deletion.
        """
        await self._admin_executor.run(_op_delete_db(name, confirm))

    async def push_schema(self, db: str, schema: SchemaDef) -> None:
        """``POST /admin/push-schema`` ``{db, schema}`` → ``{ok:true}`` (async)."""
        await self._admin_executor.run(_op_push_schema(db, schema))

    async def list_dbs(self) -> list[str]:
        """``GET /admin/dbs`` → ``{databases:[...]}`` (async)."""
        return await self._admin_executor.run(_op_list_dbs())

    async def mint_token(
        self,
        db: str,
        name: str,
        *,
        expires_at: int | None = None,
        read_only: bool = False,
        tables: list[str] | None = None,
    ) -> MintedToken:
        """``POST /admin/mint-token`` with capability fields → ``{tokenId, token}`` (async).

        Async twin of :meth:`par_rt_db.http_client.RtDbHttpClient.mint_token`;
        body semantics and capability defaults are identical.
        """
        return await self._admin_executor.run(
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
        await self._admin_executor.run(_op_revoke_token(token_id))

    async def export_db(self, db: str) -> str:
        """``GET /admin/export-db?db=<db>`` → the database snapshot as JSONL text (async)."""
        return await self._admin_executor.run(_op_export_db(db))

    async def import_db(self, db: str, jsonl: str) -> None:
        """``POST /admin/import-db?db=<db>`` with an ``application/x-ndjson`` body (async)."""
        await self._admin_executor.run(_op_import_db(db, jsonl))

    async def allowlist_add(self, db: str, email: str) -> None:
        """``POST /admin/allowlist`` ``{db, action:"add", email}`` → ``{ok:true}`` (async)."""
        await self._admin_executor.run(_op_allowlist_add(db, email))

    async def allowlist_remove(self, db: str, email: str) -> None:
        """``POST /admin/allowlist`` ``{db, action:"remove", email}`` → ``{ok:true}`` (async)."""
        await self._admin_executor.run(_op_allowlist_remove(db, email))

    async def allowlist_list(self, db: str) -> list[str]:
        """``GET /admin/allowlist?db=<db>`` → ``{emails:[...]}`` (async)."""
        return await self._admin_executor.run(_op_allowlist_list(db))

    async def admins_list(self) -> list[AdminMember]:
        """``GET /admin/admins`` → ``{admins:[{email, githubId?}]}`` (async)."""
        return await self._admin_executor.run(_op_admins_list())

    async def admins_add(self, email: str, github_id: int | None = None) -> None:
        """``POST /admin/admins`` ``{email, githubId?}`` → ``{ok:true}`` (async).

        ``githubId`` is omitted from the body when ``None`` (matches the
        server's ``skip_serializing_if`` rule).
        """
        await self._admin_executor.run(_op_admins_add(email, github_id))

    async def admins_remove(self, email: str) -> None:
        """``DELETE /admin/admins`` ``{email}`` → ``{ok:true}`` (async).

        Body-on-DELETE (axum reads it from the request body, not the URL) —
        mirrors the rust-client's ``delete_json``.
        """
        await self._admin_executor.run(_op_admins_remove(email))

    async def list_tokens(self, db: str) -> list[TokenInfo]:
        """``GET /admin/tokens?db=<db>`` → ``{tokens:[{id,name,createdAt,revoked}]}`` (async)."""
        return await self._admin_executor.run(_op_list_tokens(db))

    async def get_schema(self, db: str) -> SchemaDef:
        """``GET /admin/dbs/{db}/schema`` → the database's pushed ``SchemaDef`` (async)."""
        return await self._admin_executor.run(_op_get_schema(db))

    async def db_stats(self, db: str) -> DbStats:
        """``GET /admin/dbs/{db}/stats`` → per-table row counts + storage sizes (async)."""
        return await self._admin_executor.run(_op_db_stats(db))

    async def metrics(self) -> MetricsSnapshot:
        """``GET /admin/metrics`` → server-wide counters and gauges (async)."""
        return await self._admin_executor.run(_op_metrics())

    async def get_config(self) -> ConfigResponse:
        """``GET /admin/config`` → redacted running config + build identity + admins (async)."""
        return await self._admin_executor.run(_op_get_config())

    async def patch_config(self, patch: HotConfigPatch | Mapping[str, Any]) -> ConfigResponse:
        """``PATCH /admin/config`` with a partial hot-config body → updated config (async).

        See :meth:`par_rt_db.admin.RtDbAdminClient.patch_config` for body semantics.
        """
        return await self._admin_executor.run(_op_patch_config(patch))

    async def ops_recent(
        self,
        *,
        db: str | None = None,
        table: str | None = None,
        n: int | None = None,
    ) -> list[OpEvent]:
        """``GET /admin/ops/recent`` → recent document-op events, newest-first (async).

        All filter opts are optional; omitted filters are not sent. ``n`` caps
        the result count (server-side max 500).
        """
        return await self._admin_executor.run(_op_ops_recent(db=db, table=table, n=n))

    # --- admin data access: owner-bypass query/mutate (admin key) ---

    async def admin_query(self, db: str, query: Query | TableQuery, *, model: type = dict) -> Any:
        """``POST /admin/db/{db}/query`` ``{query}`` → parsed ``{result}`` (async).

        See :meth:`par_rt_db.admin.RtDbAdminClient.admin_query` for owner-bypass semantics.
        """
        return await self._admin_executor.run(_op_admin_query(db, query, model=model))

    async def admin_mutate(
        self,
        db: str,
        txn: Transaction,
        *,
        idempotency_key: str | None = None,
    ) -> list[StepResult]:
        """``POST /admin/db/{db}/mutate`` ``{txn, idempotencyKey?}`` → ``{results}`` (async).

        See :meth:`par_rt_db.admin.RtDbAdminClient.admin_mutate` for owner-bypass semantics.
        """
        return await self._admin_executor.run(
            _op_admin_mutate(db, txn, idempotency_key=idempotency_key)
        )

    async def migrate_schema(
        self,
        db: str,
        directives: list[Directive] | MigrateRequest,
        *,
        dry_run: bool = False,
    ) -> MigrateResult:
        """``POST /admin/db/{db}/migrate`` ``{directives, dryRun}`` → ``MigrateResult`` (async).

        See :meth:`par_rt_db.admin.RtDbAdminClient.migrate_schema` for body semantics.
        """
        return await self._admin_executor.run(_op_migrate_schema(db, directives, dry_run=dry_run))

    # --- admin control plane: managed backups (admin key) ---

    async def backup_now(self) -> None:
        """``POST /admin/backup`` → 202; one ``pg_dump`` runs in the background (async).

        Idempotent trigger guard: a second call while one is running → 409
        ``CONFLICT``. The dump runs outside the committer (``pg_dump`` is a
        read), so no document tables or subscriptions are touched.
        """
        await self._admin_executor.run(_op_backup_now())

    async def list_backups(self) -> dict[str, Any]:
        """``GET /admin/backups`` → backup list + in-progress flag (async).

        Newest-first. A missing backup dir returns an empty list rather than
        erroring — the endpoint describes what is on disk. ``running`` is the
        in-progress flag for the manual ``POST /admin/backup`` trigger.
        """
        return await self._admin_executor.run(_op_list_backups())

    async def download_backup(self, name: str) -> bytes:
        """``GET /admin/backups/{name}`` → the dump file's raw bytes (async).

        The response body is ``application/octet-stream``; do not JSON-decode.
        The server validates ``name`` (``rtdb-<stamp>.dump`` shape) before any
        filesystem access, so a traversal-shaped name is rejected at the edge.
        """
        return await self._admin_executor.run(_op_download_backup(name))

    async def delete_backup(self, name: str) -> None:
        """``DELETE /admin/backups/{name}`` → 204; removes one dump file (async).

        Same ``validate_dump_name`` short-circuit as download; 404 if the file
        is already gone.
        """
        await self._admin_executor.run(_op_delete_backup(name))

    async def restore_backup(self, name: str) -> dict[str, Any]:
        """``POST /admin/restore`` ``{name, confirm}`` → ``{target, instructions}`` (async).

        ``confirm`` is sent equal to ``name`` (typed guard, mirroring
        ``delete_db``). The live DB is never touched: restore creates a fresh
        ``rtdb_restored_<stamp>`` DB and ``pg_restore``s into it. The response
        carries the target DB name and cutover instructions.
        """
        return await self._admin_executor.run(_op_restore_backup(name))

    # --- request plumbing ---

    async def _send(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Issue a request; raise ``RtDbError`` on non-2xx, else return response.

        The bearer header is set on the underlying ``httpx.AsyncClient``
        (constructor time), so callers pass only the method, path, and
        per-request kwargs (``json``/``content``/``headers``/``params``).
        """
        resp = await self._client.request(method, path, **kwargs)
        if not resp.is_success:
            raise RtDbError.from_http(resp.status_code, resp.content)
        return resp

    @staticmethod
    def _expect_ok(resp: httpx.Response) -> None:
        """Assert the body is ``{ok: true}``; raise ``RtDbError`` otherwise."""
        if not resp.json().get("ok"):
            raise RtDbError(ErrorCode.INTERNAL, "admin request returned ok=false")
