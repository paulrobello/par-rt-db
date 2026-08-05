"""Async HTTP/admin/storage client for par-rt-db (the ``[aio]`` extra).

A one-to-one async mirror of :class:`par_rt_db.http_client.RtDbHttpClient` over
:class:`httpx.AsyncClient`: every public method is ``async def`` and every
request is ``await``-ed. Wire types, DSL builders, and the response models are
re-imported from :mod:`par_rt_db.http_client` and the shared modules — nothing
is redefined. ``httpx`` is imported lazily inside ``__init__`` so this module
imports without the ``[aio]`` extra installed.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from .errors import ErrorCode, RtDbError
from .migration import Directive, MigrateRequest
from .mutation import StepResult, Transaction
from .query import Query, TableQuery, _terminal_of, parse_result
from .schema import SchemaDef

if TYPE_CHECKING:
    import httpx

# Re-import the shared response models + adapters this module references.
from .http_client import (
    _BATCH_ADAPTER,
    _SCHEDULES_ADAPTER,
    _STEP_RESULT_ADAPTER,
    AdminMember,
    ConfigResponse,
    DbStats,
    FileMetadata,
    HotConfigPatch,
    MetricsSnapshot,
    MigrateResult,
    MintedToken,
    OpEvent,
    TokenInfo,
    UploadResult,
)
from .wire import BatchQueryOutcome, ScheduleInfo, ScheduleWhen


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

        ``when`` is a ``ScheduleWhen`` (``_AfterMs``/``_RunAt``/``_Cron`` from
        ``par_rt_db.wire``). One-shot jobs past due run immediately; cron jobs
        skip missed windows (server-side semantics).
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

    async def upload(self, data: bytes, *, content_type: str | None = None) -> UploadResult:
        """``POST /api/storage/{db}`` with a raw body → server-computed metadata.

        ``content_type`` sets the ``Content-Type`` header AND is stored as the
        file's type; when ``None`` the header is left unset (httpx defaults to
        ``application/octet-stream``). Unlike every other method the body is NOT
        JSON.
        """
        headers = {"Content-Type": content_type} if content_type is not None else None
        resp = await self._send(
            "POST",
            f"/api/storage/{self._db}",
            content=data,
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

    def get_url(self, id: str) -> str:
        """The public serve URL (``GET /storage/{id}``) — no request is made."""
        return f"{self._base}/storage/{id}"

    # --- admin control plane (admin key as the token) ---

    async def create_db(self, name: str) -> None:
        """``POST /admin/create-db`` ``{name}`` → ``{ok:true}``."""
        resp = await self._send("POST", "/admin/create-db", json={"name": name})
        self._expect_ok(resp)

    async def delete_db(self, name: str, confirm: str) -> None:
        """``POST /admin/delete-db`` ``{name, confirm}`` → ``{ok:true}``.

        The server rejects with ``BAD_REQUEST`` unless ``confirm == name``
        exactly — the typed confirmation guard against accidental deletion.
        """
        resp = await self._send(
            "POST",
            "/admin/delete-db",
            json={"name": name, "confirm": confirm},
        )
        self._expect_ok(resp)

    async def push_schema(self, db: str, schema: SchemaDef) -> None:
        """``POST /admin/push-schema`` ``{db, schema}`` → ``{ok:true}``."""
        resp = await self._send(
            "POST",
            "/admin/push-schema",
            json={"db": db, "schema": schema.model_dump(by_alias=True, mode="json")},
        )
        self._expect_ok(resp)

    async def list_dbs(self) -> list[str]:
        """``GET /admin/dbs`` → ``{databases:[...]}``."""
        resp = await self._send("GET", "/admin/dbs")
        return list(resp.json()["databases"])

    async def mint_token(
        self,
        db: str,
        name: str,
        *,
        expires_at: int | None = None,
        read_only: bool = False,
        tables: list[str] | None = None,
    ) -> MintedToken:
        """``POST /admin/mint-token`` with capability fields → ``{tokenId, token}``.

        Async twin of :meth:`par_rt_db.http_client.RtDbHttpClient.mint_token`;
        body semantics and capability defaults are identical.
        """
        body: dict[str, Any] = {"db": db, "name": name, "readOnly": read_only}
        if expires_at is not None:
            body["expiresAt"] = expires_at
        if tables is not None:
            body["tables"] = list(tables)
        resp = await self._send("POST", "/admin/mint-token", json=body)
        return MintedToken.model_validate(resp.json())

    async def revoke_token(self, token_id: str) -> None:
        """``POST /admin/revoke-token`` ``{tokenId}`` → ``{ok:true}``."""
        resp = await self._send("POST", "/admin/revoke-token", json={"tokenId": token_id})
        self._expect_ok(resp)

    async def export_db(self, db: str) -> str:
        """``GET /admin/export-db?db=<db>`` → the database snapshot as JSONL text."""
        return (await self._send("GET", "/admin/export-db", params={"db": db})).text

    async def import_db(self, db: str, jsonl: str) -> None:
        """``POST /admin/import-db?db=<db>`` with an ``application/x-ndjson`` body."""
        resp = await self._send(
            "POST",
            "/admin/import-db",
            params={"db": db},
            content=jsonl,
            headers={"Content-Type": "application/x-ndjson"},
        )
        self._expect_ok(resp)

    async def allowlist_add(self, db: str, email: str) -> None:
        """``POST /admin/allowlist`` ``{db, action:"add", email}`` → ``{ok:true}``."""
        resp = await self._send(
            "POST",
            "/admin/allowlist",
            json={"db": db, "action": "add", "email": email},
        )
        self._expect_ok(resp)

    async def allowlist_remove(self, db: str, email: str) -> None:
        """``POST /admin/allowlist`` ``{db, action:"remove", email}`` → ``{ok:true}``."""
        resp = await self._send(
            "POST",
            "/admin/allowlist",
            json={"db": db, "action": "remove", "email": email},
        )
        self._expect_ok(resp)

    async def allowlist_list(self, db: str) -> list[str]:
        """``GET /admin/allowlist?db=<db>`` → ``{emails:[...]}``."""
        resp = await self._send("GET", "/admin/allowlist", params={"db": db})
        return list(resp.json()["emails"])

    async def admins_list(self) -> list[AdminMember]:
        """``GET /admin/admins`` → ``{admins:[{email, githubId?}]}``."""
        resp = await self._send("GET", "/admin/admins")
        return [AdminMember.model_validate(m) for m in resp.json()["admins"]]

    async def admins_add(self, email: str, github_id: int | None = None) -> None:
        """``POST /admin/admins`` ``{email, githubId?}`` → ``{ok:true}``.

        ``githubId`` is omitted from the body when ``None`` (matches the
        server's ``skip_serializing_if`` rule).
        """
        body: dict[str, Any] = {"email": email}
        if github_id is not None:
            body["githubId"] = github_id
        resp = await self._send("POST", "/admin/admins", json=body)
        self._expect_ok(resp)

    async def admins_remove(self, email: str) -> None:
        """``DELETE /admin/admins`` ``{email}`` → ``{ok:true}``.

        Body-on-DELETE (axum reads it from the request body, not the URL) —
        mirrors the rust-client's ``delete_json``.
        """
        resp = await self._send("DELETE", "/admin/admins", json={"email": email})
        self._expect_ok(resp)

    async def list_tokens(self, db: str) -> list[TokenInfo]:
        """``GET /admin/tokens?db=<db>`` → ``{tokens:[{id,name,createdAt,revoked}]}``."""
        resp = await self._send("GET", "/admin/tokens", params={"db": db})
        return [TokenInfo.model_validate(t) for t in resp.json()["tokens"]]

    async def get_schema(self, db: str) -> SchemaDef:
        """``GET /admin/dbs/{db}/schema`` → the database's pushed ``SchemaDef``."""
        resp = await self._send("GET", f"/admin/dbs/{db}/schema")
        return SchemaDef.model_validate(resp.json())

    async def db_stats(self, db: str) -> DbStats:
        """``GET /admin/dbs/{db}/stats`` → per-table row counts + storage sizes."""
        resp = await self._send("GET", f"/admin/dbs/{db}/stats")
        return DbStats.model_validate(resp.json())

    async def metrics(self) -> MetricsSnapshot:
        """``GET /admin/metrics`` → server-wide counters and gauges."""
        resp = await self._send("GET", "/admin/metrics")
        return MetricsSnapshot.model_validate(resp.json())

    async def get_config(self) -> ConfigResponse:
        """``GET /admin/config`` → redacted running config + build identity + admins."""
        resp = await self._send("GET", "/admin/config")
        return ConfigResponse.model_validate(resp.json())

    async def patch_config(self, patch: HotConfigPatch | Mapping[str, Any]) -> ConfigResponse:
        """``PATCH /admin/config`` with a partial hot-config body → updated config.

        Each present field fully replaces the prior value; the server validates
        (``sessionTtlDays>=1``, ``maxFileSize`` within bounds, origin shape).
        Accepts a ``HotConfigPatch`` model or a plain ``Mapping`` of wire camelCase
        keys (e.g. ``{"sessionTtlDays": 60}``); ``None``-valued model fields are
        omitted from the body (matches rust-client's ``skip_serializing_if``).
        """
        if isinstance(patch, Mapping):
            body: dict[str, Any] = dict(patch)
        else:
            body = patch.model_dump(by_alias=True, mode="json", exclude_none=True)
        resp = await self._send("PATCH", "/admin/config", json=body)
        return ConfigResponse.model_validate(resp.json())

    async def ops_recent(
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
        params: dict[str, Any] = {}
        if db is not None:
            params["db"] = db
        if table is not None:
            params["table"] = table
        if n is not None:
            params["n"] = n
        resp = await self._send("GET", "/admin/ops/recent", params=params)
        return [OpEvent.model_validate(e) for e in resp.json()["ops"]]

    # --- admin data access: owner-bypass query/mutate (admin key) ---

    async def admin_query(self, db: str, query: Query | TableQuery, *, model: type = dict) -> Any:
        """``POST /admin/db/{db}/query`` ``{query}`` → parsed ``{result}``.

        Owner-bypass: an admin reads documents across every database regardless
        of ``ownerField``. ``db`` rides in the URL (singular ``db``), so the body
        omits it. Result parsing mirrors ``run``.
        """
        built = query.build() if isinstance(query, TableQuery) else query
        body = {"query": built.model_dump(by_alias=True, mode="json")}
        resp = await self._send("POST", f"/admin/db/{db}/query", json=body)
        result = resp.json()["result"]
        return parse_result(model, _terminal_of(built), result)

    async def admin_mutate(
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
        body: dict[str, Any] = {"txn": txn.model_dump(by_alias=True, mode="json")}
        if idempotency_key is not None:
            body["idempotencyKey"] = idempotency_key
        resp = await self._send("POST", f"/admin/db/{db}/mutate", json=body)
        results = resp.json()["results"]
        return [_STEP_RESULT_ADAPTER.validate_python(r) for r in results]

    async def migrate_schema(
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
        if isinstance(directives, MigrateRequest):
            wire_directives = directives.directives
        else:
            wire_directives = directives
        body = {
            "directives": [d.model_dump(by_alias=True, mode="json") for d in wire_directives],
            "dryRun": dry_run,
        }
        resp = await self._send("POST", f"/admin/db/{db}/migrate", json=body)
        return MigrateResult.model_validate(resp.json())

    # --- admin control plane: managed backups (admin key) ---

    async def backup_now(self) -> None:
        """``POST /admin/backup`` → 202; one ``pg_dump`` runs in the background.

        Idempotent trigger guard: a second call while one is running → 409
        ``CONFLICT``. The dump runs outside the committer (``pg_dump`` is a
        read), so no document tables or subscriptions are touched.
        """
        resp = await self._send("POST", "/admin/backup", json={})
        self._expect_ok(resp)

    async def list_backups(self) -> dict[str, Any]:
        """``GET /admin/backups`` → ``{running: bool, backups: [{name, sizeBytes, createdMs}]}``.

        Newest-first. A missing backup dir returns an empty list rather than
        erroring — the endpoint describes what is on disk. ``running`` is the
        in-progress flag for the manual ``POST /admin/backup`` trigger.
        """
        resp = await self._send("GET", "/admin/backups")
        return dict(resp.json())

    async def download_backup(self, name: str) -> bytes:
        """``GET /admin/backups/{name}`` → the dump file's raw bytes.

        The response body is ``application/octet-stream``; do not JSON-decode.
        The server validates ``name`` (``rtdb-<stamp>.dump`` shape) before any
        filesystem access, so a traversal-shaped name is rejected at the edge.
        """
        resp = await self._send("GET", f"/admin/backups/{name}")
        return resp.content

    async def delete_backup(self, name: str) -> None:
        """``DELETE /admin/backups/{name}`` → 204; removes one dump file.

        Same ``validate_dump_name`` short-circuit as download; 404 if the file
        is already gone.
        """
        await self._send("DELETE", f"/admin/backups/{name}")

    async def restore_backup(self, name: str) -> dict[str, Any]:
        """``POST /admin/restore`` ``{name, confirm}`` → ``{target, instructions}``.

        ``confirm`` is sent equal to ``name`` (typed guard, mirroring
        ``delete_db``). The live DB is never touched: restore creates a fresh
        ``rtdb_restored_<stamp>`` DB and ``pg_restore``s into it. The response
        carries the target DB name and cutover instructions.
        """
        resp = await self._send(
            "POST",
            "/admin/restore",
            json={"name": name, "confirm": name},
        )
        return dict(resp.json())

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
