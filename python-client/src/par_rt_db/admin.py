"""Admin control-plane client for par-rt-db.

Dedicated admin-key bearer client for the full ``/admin/*`` surface.
Authenticates every call with the instance admin key and mirrors the whole
admin control plane found on :class:`par_rt_db.http_client.RtDbHttpClient`:

* db lifecycle — ``create-db`` / ``delete-db`` / ``dbs``
* schema — ``push-schema`` / ``dbs/{db}/schema`` / ``dbs/{db}/migrate``
* export / import — ``export-db`` / ``import-db``
* allowlist — ``/admin/allowlist`` (add / remove / list)
* admins — ``/admin/admins`` (list / add / remove)
* introspection — ``dbs/{db}/stats`` / ``metrics`` / ``ops/recent`` /
  ``config`` (get + patch)
* owner-bypass data access — ``/admin/db/{db}/query|mutate``
* managed backups — ``backup`` / ``backups`` / ``restore``
* token management (ENH-005) — ``mint-token`` / ``revoke-token`` / ``tokens``
  with the capability fields (``expiresAt``, ``readOnly``, ``tables``)
* webhook management (ENH-003) — ``/admin/db/{db}/webhooks`` (list / create /
  edit / delete) plus ``.../{id}/deliveries`` for the delivery outbox

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
canonical pydantic models re-exported from :mod:`par_rt_db.http_client`, so an
``isinstance`` check against the top-level ``from par_rt_db import MintedToken``
succeeds regardless of which client produced the value — there is exactly one
model type per response shape across the sync/async data-plane and admin
clients.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from pydantic import TypeAdapter

from .errors import ErrorCode, RtDbError
from .http_client import (
    AdminMember,
    ConfigResponse,
    DbStats,
    HotConfigPatch,
    MetricsSnapshot,
    MigrateResult,
    MintedToken,
    OpEvent,
    TokenInfo,
    Webhook,
    WebhookDelivery,
)
from .migration import Directive, MigrateRequest
from .mutation import StepResult, Transaction
from .query import Query, TableQuery, _terminal_of, parse_result
from .schema import SchemaDef

if TYPE_CHECKING:
    import httpx


# Sentinel for ``edit_webhook`` kwargs that distinguishes "caller did not pass
# this kwarg" (``_UNSET`` → omit from the body → server leaves the field
# unchanged) from "caller passed ``None``" (``table=None`` → send JSON ``null``
# → server clears the field). Only ``edit_webhook`` needs the tri-state —
# ``create_webhook`` treats ``table=None`` as all-tables (matching the server's
# create semantics), so it does not use the sentinel.
_UNSET: Any = object()


# ``StepResult`` is a ``Union`` alias (no ``model_validate``); route through a
# single ``TypeAdapter`` for the untagged per-step result, mirroring mutation.py
# and http_client.py.
_STEP_RESULT_ADAPTER = TypeAdapter(StepResult)


class RtDbAdminClient:
    """Sync admin control-plane client (the ``[http]`` extra).

    Authenticates every call with the instance admin key (bearer). Construct
    with the admin key and use as a context manager to close the underlying
    :class:`httpx.Client`::

        with RtDbAdminClient(url, admin_key) as c:
            minted = c.mint_token("mydb", "scraper", read_only=True, tables=["users"])
            c.push_schema("mydb", schema)
            stats = c.db_stats("mydb")

    Full admin surface: db lifecycle, schema push/get/migrate, export/import,
    allowlist + admin-member management, introspection (stats/metrics/ops/
    config), owner-bypass query/mutate, managed backups, and the ENH-005 token
    triple. Routes, bodies, and response models are byte-identical with the
    server (``server/src/admin.rs``) and with
    :class:`par_rt_db.http_client.RtDbHttpClient`.
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
            headers={"Authorization": f"Bearer {admin_key}"},
            transport=transport,
        )

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
        resp = self._req("POST", "/admin/create-db", json={"name": name})
        self._expect_ok(resp)

    def delete_db(self, name: str, confirm: str) -> None:
        """``POST /admin/delete-db`` ``{name, confirm}`` → ``{ok:true}``.

        The server rejects with ``BAD_REQUEST`` unless ``confirm == name``
        exactly — the typed confirmation guard against accidental deletion.
        """
        resp = self._req(
            "POST",
            "/admin/delete-db",
            json={"name": name, "confirm": confirm},
        )
        self._expect_ok(resp)

    def list_dbs(self) -> list[str]:
        """``GET /admin/dbs`` → ``{databases:[...]}``."""
        resp = self._req("GET", "/admin/dbs")
        return list(resp.json()["databases"])

    # --- schema ---

    def push_schema(self, db: str, schema: SchemaDef) -> None:
        """``POST /admin/push-schema`` ``{db, schema}`` → ``{ok:true}``."""
        resp = self._req(
            "POST",
            "/admin/push-schema",
            json={"db": db, "schema": schema.model_dump(by_alias=True, mode="json")},
        )
        self._expect_ok(resp)

    def get_schema(self, db: str) -> SchemaDef:
        """``GET /admin/dbs/{db}/schema`` → the database's pushed ``SchemaDef``."""
        resp = self._req("GET", f"/admin/dbs/{db}/schema")
        return SchemaDef.model_validate(resp.json())

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
        if isinstance(directives, MigrateRequest):
            wire_directives = directives.directives
        else:
            wire_directives = directives
        body = {
            "directives": [d.model_dump(by_alias=True, mode="json") for d in wire_directives],
            "dryRun": dry_run,
        }
        resp = self._req("POST", f"/admin/db/{db}/migrate", json=body)
        return MigrateResult.model_validate(resp.json())

    # --- export / import ---

    def export_db(self, db: str) -> str:
        """``GET /admin/export-db?db=<db>`` → the database snapshot as JSONL text."""
        return self._req("GET", "/admin/export-db", params={"db": db}).text

    def import_db(self, db: str, jsonl: str) -> None:
        """``POST /admin/import-db?db=<db>`` with an ``application/x-ndjson`` body."""
        resp = self._req(
            "POST",
            "/admin/import-db",
            params={"db": db},
            content=jsonl,
            headers={"Content-Type": "application/x-ndjson"},
        )
        self._expect_ok(resp)

    # --- allowlist ---

    def allowlist_add(self, db: str, email: str) -> None:
        """``POST /admin/allowlist`` ``{db, action:"add", email}`` → ``{ok:true}``."""
        resp = self._req(
            "POST",
            "/admin/allowlist",
            json={"db": db, "action": "add", "email": email},
        )
        self._expect_ok(resp)

    def allowlist_remove(self, db: str, email: str) -> None:
        """``POST /admin/allowlist`` ``{db, action:"remove", email}`` → ``{ok:true}``."""
        resp = self._req(
            "POST",
            "/admin/allowlist",
            json={"db": db, "action": "remove", "email": email},
        )
        self._expect_ok(resp)

    def allowlist_list(self, db: str) -> list[str]:
        """``GET /admin/allowlist?db=<db>`` → ``{emails:[...]}``."""
        resp = self._req("GET", "/admin/allowlist", params={"db": db})
        return list(resp.json()["emails"])

    # --- admins ---

    def admins_list(self) -> list[AdminMember]:
        """``GET /admin/admins`` → ``{admins:[{email, githubId?}]}``."""
        resp = self._req("GET", "/admin/admins")
        return [AdminMember.model_validate(m) for m in resp.json()["admins"]]

    def admins_add(self, email: str, github_id: int | None = None) -> None:
        """``POST /admin/admins`` ``{email, githubId?}`` → ``{ok:true}``.

        ``githubId`` is omitted from the body when ``None`` (matches the
        server's ``skip_serializing_if`` rule).
        """
        body: dict[str, Any] = {"email": email}
        if github_id is not None:
            body["githubId"] = github_id
        resp = self._req("POST", "/admin/admins", json=body)
        self._expect_ok(resp)

    def admins_remove(self, email: str) -> None:
        """``DELETE /admin/admins`` ``{email}`` → ``{ok:true}``.

        Body-on-DELETE (axum reads it from the request body, not the URL) —
        mirrors the rust-client's ``delete_json``.
        """
        resp = self._req("DELETE", "/admin/admins", json={"email": email})
        self._expect_ok(resp)

    # --- introspection ---

    def db_stats(self, db: str) -> DbStats:
        """``GET /admin/dbs/{db}/stats`` → per-table row counts + storage sizes."""
        resp = self._req("GET", f"/admin/dbs/{db}/stats")
        return DbStats.model_validate(resp.json())

    def metrics(self) -> MetricsSnapshot:
        """``GET /admin/metrics`` → server-wide counters and gauges."""
        resp = self._req("GET", "/admin/metrics")
        return MetricsSnapshot.model_validate(resp.json())

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
        params: dict[str, Any] = {}
        if db is not None:
            params["db"] = db
        if table is not None:
            params["table"] = table
        if n is not None:
            params["n"] = n
        resp = self._req("GET", "/admin/ops/recent", params=params)
        return [OpEvent.model_validate(e) for e in resp.json()["ops"]]

    # --- config ---

    def get_config(self) -> ConfigResponse:
        """``GET /admin/config`` → redacted running config + build identity + admins."""
        resp = self._req("GET", "/admin/config")
        return ConfigResponse.model_validate(resp.json())

    def patch_config(self, patch: HotConfigPatch | Mapping[str, Any]) -> ConfigResponse:
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
        resp = self._req("PATCH", "/admin/config", json=body)
        return ConfigResponse.model_validate(resp.json())

    # --- owner-bypass data access: admin query/mutate ---

    def admin_query(self, db: str, query: Query | TableQuery, *, model: type = dict) -> Any:
        """``POST /admin/db/{db}/query`` ``{query}`` → parsed ``{result}``.

        Owner-bypass: an admin reads documents across every database regardless
        of ``ownerField``. ``db`` rides in the URL (singular ``db``), so the body
        omits it. Result parsing mirrors ``RtDbHttpClient.run``.
        """
        built = query.build() if isinstance(query, TableQuery) else query
        body = {"query": built.model_dump(by_alias=True, mode="json")}
        resp = self._req("POST", f"/admin/db/{db}/query", json=body)
        result = resp.json()["result"]
        return parse_result(model, _terminal_of(built), result)

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
        body: dict[str, Any] = {"txn": txn.model_dump(by_alias=True, mode="json")}
        if idempotency_key is not None:
            body["idempotencyKey"] = idempotency_key
        resp = self._req("POST", f"/admin/db/{db}/mutate", json=body)
        results = resp.json()["results"]
        return [_STEP_RESULT_ADAPTER.validate_python(r) for r in results]

    # --- managed backups ---

    def backup_now(self) -> None:
        """``POST /admin/backup`` → 202; one ``pg_dump`` runs in the background.

        Idempotent trigger guard: a second call while one is running → 409
        ``CONFLICT``. The dump runs outside the committer (``pg_dump`` is a
        read), so no document tables or subscriptions are touched.
        """
        resp = self._req("POST", "/admin/backup", json={})
        self._expect_ok(resp)

    def list_backups(self) -> dict[str, Any]:
        """``GET /admin/backups`` → ``{running: bool, backups: [{name, sizeBytes, createdMs}]}``.

        Newest-first. A missing backup dir returns an empty list rather than
        erroring — the endpoint describes what is on disk. ``running`` is the
        in-progress flag for the manual ``POST /admin/backup`` trigger.
        """
        resp = self._req("GET", "/admin/backups")
        return dict(resp.json())

    def download_backup(self, name: str) -> bytes:
        """``GET /admin/backups/{name}`` → the dump file's raw bytes.

        The response body is ``application/octet-stream``; do not JSON-decode.
        The server validates ``name`` (``rtdb-<stamp>.dump`` shape) before any
        filesystem access, so a traversal-shaped name is rejected at the edge.
        """
        resp = self._req("GET", f"/admin/backups/{name}")
        return resp.content

    def delete_backup(self, name: str) -> None:
        """``DELETE /admin/backups/{name}`` → 204; removes one dump file.

        Same ``validate_dump_name`` short-circuit as download; 404 if the file
        is already gone.
        """
        self._req("DELETE", f"/admin/backups/{name}")

    def restore_backup(self, name: str) -> dict[str, Any]:
        """``POST /admin/restore`` ``{name, confirm}`` → ``{target, instructions}``.

        ``confirm`` is sent equal to ``name`` (typed guard, mirroring
        ``delete_db``). The live DB is never touched: restore creates a fresh
        ``rtdb_restored_<stamp>`` DB and ``pg_restore``s into it. The response
        carries the target DB name and cutover instructions.
        """
        resp = self._req(
            "POST",
            "/admin/restore",
            json={"name": name, "confirm": name},
        )
        return dict(resp.json())

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
        body: dict[str, Any] = {"db": db, "name": name, "readOnly": read_only}
        if expires_at is not None:
            body["expiresAt"] = expires_at
        if tables is not None:
            body["tables"] = list(tables)
        resp = self._req("POST", "/admin/mint-token", json=body)
        return MintedToken.model_validate(resp.json())

    def revoke_token(self, token_id: str) -> None:
        """``POST /admin/revoke-token`` ``{tokenId}`` → ``{ok:true}``."""
        resp = self._req("POST", "/admin/revoke-token", json={"tokenId": token_id})
        self._expect_ok(resp)

    def list_tokens(self, db: str) -> list[TokenInfo]:
        """``GET /admin/tokens?db=<db>`` → ``[TokenInfo, ...]``."""
        resp = self._req("GET", "/admin/tokens", params={"db": db})
        return [TokenInfo.model_validate(t) for t in resp.json()["tokens"]]

    # --- webhook surface (ENH-003) ---

    def list_webhooks(self, db: str) -> list[Webhook]:
        """``GET /admin/db/{db}/webhooks`` → ``{webhooks:[...]}``.

        Returns an empty list when webhooks are disabled at boot (the server
        permits the table to not exist), and for a db with no webhooks.
        """
        resp = self._req("GET", f"/admin/db/{db}/webhooks")
        return [Webhook.model_validate(w) for w in resp.json()["webhooks"]]

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
        body: dict[str, Any] = {"url": url}
        if table is not None:
            body["table"] = table
        if events is not None:
            body["events"] = list(events)
        if enabled is not None:
            body["enabled"] = enabled
        resp = self._req("POST", f"/admin/db/{db}/webhooks", json=body)
        return int(resp.json()["id"])

    def edit_webhook(
        self,
        db: str,
        id: int,
        *,
        url: str | None = None,
        table: str | None | object = _UNSET,
        events: list[str] | None = None,
        enabled: bool | None = None,
    ) -> Webhook:
        """``PUT /admin/db/{db}/webhooks/{id}`` → the updated :class:`Webhook`.

        Each kwarg is independently optional on the wire — only kwargs the
        caller passes are sent, omitted fields are left unchanged by the
        server. ``table`` is a tri-state kwarg (the load-bearing case):

        * omitted (``_UNSET``) → omitted from the body → table filter unchanged
        * ``None`` → sent as JSON ``null`` → clears to all-tables
        * ``"items"`` → sent as ``"items"`` → set to that table

        ``url``/``events``/``enabled`` use a plain ``None`` default (their
        distinguishing value would be the empty string / empty list / etc., so
        a sentinel is unnecessary): ``None`` means "not passed" → omitted from
        the body → unchanged; pass a real value to set it.
        """
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
        resp = self._req("PUT", f"/admin/db/{db}/webhooks/{id}", json=body)
        return Webhook.model_validate(resp.json())

    def delete_webhook(self, db: str, id: int) -> None:
        """``DELETE /admin/db/{db}/webhooks/{id}`` → ``{ok:true}``.

        Cascading pending deliveries via the FK. A non-numeric id is a 400 on
        the server; a missing id is a 404 (returns ``ok:false`` → raises).
        """
        resp = self._req("DELETE", f"/admin/db/{db}/webhooks/{id}")
        self._expect_ok(resp)

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
        params: dict[str, Any] = {}
        if status is not None:
            params["status"] = status
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        resp = self._req(
            "GET",
            f"/admin/db/{db}/webhooks/{id}/deliveries",
            params=params,
        )
        return [WebhookDelivery.model_validate(d) for d in resp.json()["deliveries"]]

    # --- request plumbing ---

    def _req(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Issue a request; raise :class:`RtDbError` on non-2xx, else return it.

        The admin bearer header is set on the underlying :class:`httpx.Client`
        at construction time, so callers pass only the method, path, and
        per-request kwargs (``json``/``params``/``content``/``headers``).
        """
        resp = self._client.request(method, path, **kwargs)
        if not resp.is_success:
            raise RtDbError.from_http(resp.status_code, resp.content)
        return resp

    @staticmethod
    def _expect_ok(resp: httpx.Response) -> None:
        """Assert the body is ``{ok: true}``; raise :class:`RtDbError` otherwise."""
        if not resp.json().get("ok"):
            raise RtDbError(ErrorCode.INTERNAL, "admin request returned ok=false")


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
            headers={"Authorization": f"Bearer {admin_key}"},
            transport=transport,
        )

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
        resp = await self._req("POST", "/admin/create-db", json={"name": name})
        self._expect_ok(resp)

    async def delete_db(self, name: str, confirm: str) -> None:
        """``POST /admin/delete-db`` ``{name, confirm}`` → ``{ok:true}`` (async).

        See :meth:`RtDbAdminClient.delete_db` for the typed-confirm guard.
        """
        resp = await self._req(
            "POST",
            "/admin/delete-db",
            json={"name": name, "confirm": confirm},
        )
        self._expect_ok(resp)

    async def list_dbs(self) -> list[str]:
        """``GET /admin/dbs`` → ``{databases:[...]}`` (async)."""
        resp = await self._req("GET", "/admin/dbs")
        return list(resp.json()["databases"])

    # --- schema ---

    async def push_schema(self, db: str, schema: SchemaDef) -> None:
        """``POST /admin/push-schema`` ``{db, schema}`` → ``{ok:true}`` (async)."""
        resp = await self._req(
            "POST",
            "/admin/push-schema",
            json={"db": db, "schema": schema.model_dump(by_alias=True, mode="json")},
        )
        self._expect_ok(resp)

    async def get_schema(self, db: str) -> SchemaDef:
        """``GET /admin/dbs/{db}/schema`` → the database's pushed ``SchemaDef`` (async)."""
        resp = await self._req("GET", f"/admin/dbs/{db}/schema")
        return SchemaDef.model_validate(resp.json())

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
        if isinstance(directives, MigrateRequest):
            wire_directives = directives.directives
        else:
            wire_directives = directives
        body = {
            "directives": [d.model_dump(by_alias=True, mode="json") for d in wire_directives],
            "dryRun": dry_run,
        }
        resp = await self._req("POST", f"/admin/db/{db}/migrate", json=body)
        return MigrateResult.model_validate(resp.json())

    # --- export / import ---

    async def export_db(self, db: str) -> str:
        """``GET /admin/export-db?db=<db>`` → the database snapshot as JSONL text (async)."""
        resp = await self._req("GET", "/admin/export-db", params={"db": db})
        return resp.text

    async def import_db(self, db: str, jsonl: str) -> None:
        """``POST /admin/import-db?db=<db>`` with an ``application/x-ndjson`` body (async)."""
        resp = await self._req(
            "POST",
            "/admin/import-db",
            params={"db": db},
            content=jsonl,
            headers={"Content-Type": "application/x-ndjson"},
        )
        self._expect_ok(resp)

    # --- allowlist ---

    async def allowlist_add(self, db: str, email: str) -> None:
        """``POST /admin/allowlist`` ``{db, action:"add", email}`` → ``{ok:true}`` (async)."""
        resp = await self._req(
            "POST",
            "/admin/allowlist",
            json={"db": db, "action": "add", "email": email},
        )
        self._expect_ok(resp)

    async def allowlist_remove(self, db: str, email: str) -> None:
        """``POST /admin/allowlist`` ``{db, action:"remove", email}`` → ``{ok:true}`` (async)."""
        resp = await self._req(
            "POST",
            "/admin/allowlist",
            json={"db": db, "action": "remove", "email": email},
        )
        self._expect_ok(resp)

    async def allowlist_list(self, db: str) -> list[str]:
        """``GET /admin/allowlist?db=<db>`` → ``{emails:[...]}`` (async)."""
        resp = await self._req("GET", "/admin/allowlist", params={"db": db})
        return list(resp.json()["emails"])

    # --- admins ---

    async def admins_list(self) -> list[AdminMember]:
        """``GET /admin/admins`` → ``{admins:[{email, githubId?}]}`` (async)."""
        resp = await self._req("GET", "/admin/admins")
        return [AdminMember.model_validate(m) for m in resp.json()["admins"]]

    async def admins_add(self, email: str, github_id: int | None = None) -> None:
        """``POST /admin/admins`` ``{email, githubId?}`` → ``{ok:true}`` (async).

        See :meth:`RtDbAdminClient.admins_add` for the ``githubId`` omission rule.
        """
        body: dict[str, Any] = {"email": email}
        if github_id is not None:
            body["githubId"] = github_id
        resp = await self._req("POST", "/admin/admins", json=body)
        self._expect_ok(resp)

    async def admins_remove(self, email: str) -> None:
        """``DELETE /admin/admins`` ``{email}`` → ``{ok:true}`` (async).

        Body-on-DELETE — mirrors the rust-client's ``delete_json``.
        """
        resp = await self._req("DELETE", "/admin/admins", json={"email": email})
        self._expect_ok(resp)

    # --- introspection ---

    async def db_stats(self, db: str) -> DbStats:
        """``GET /admin/dbs/{db}/stats`` → per-table row counts + storage sizes (async)."""
        resp = await self._req("GET", f"/admin/dbs/{db}/stats")
        return DbStats.model_validate(resp.json())

    async def metrics(self) -> MetricsSnapshot:
        """``GET /admin/metrics`` → server-wide counters and gauges (async)."""
        resp = await self._req("GET", "/admin/metrics")
        return MetricsSnapshot.model_validate(resp.json())

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
        params: dict[str, Any] = {}
        if db is not None:
            params["db"] = db
        if table is not None:
            params["table"] = table
        if n is not None:
            params["n"] = n
        resp = await self._req("GET", "/admin/ops/recent", params=params)
        return [OpEvent.model_validate(e) for e in resp.json()["ops"]]

    # --- config ---

    async def get_config(self) -> ConfigResponse:
        """``GET /admin/config`` → redacted running config + build identity + admins (async)."""
        resp = await self._req("GET", "/admin/config")
        return ConfigResponse.model_validate(resp.json())

    async def patch_config(self, patch: HotConfigPatch | Mapping[str, Any]) -> ConfigResponse:
        """``PATCH /admin/config`` with a partial hot-config body → updated config (async).

        See :meth:`RtDbAdminClient.patch_config` for body semantics.
        """
        if isinstance(patch, Mapping):
            body: dict[str, Any] = dict(patch)
        else:
            body = patch.model_dump(by_alias=True, mode="json", exclude_none=True)
        resp = await self._req("PATCH", "/admin/config", json=body)
        return ConfigResponse.model_validate(resp.json())

    # --- owner-bypass data access: admin query/mutate ---

    async def admin_query(self, db: str, query: Query | TableQuery, *, model: type = dict) -> Any:
        """``POST /admin/db/{db}/query`` ``{query}`` → parsed ``{result}`` (async).

        See :meth:`RtDbAdminClient.admin_query` for owner-bypass semantics.
        """
        built = query.build() if isinstance(query, TableQuery) else query
        body = {"query": built.model_dump(by_alias=True, mode="json")}
        resp = await self._req("POST", f"/admin/db/{db}/query", json=body)
        result = resp.json()["result"]
        return parse_result(model, _terminal_of(built), result)

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
        body: dict[str, Any] = {"txn": txn.model_dump(by_alias=True, mode="json")}
        if idempotency_key is not None:
            body["idempotencyKey"] = idempotency_key
        resp = await self._req("POST", f"/admin/db/{db}/mutate", json=body)
        results = resp.json()["results"]
        return [_STEP_RESULT_ADAPTER.validate_python(r) for r in results]

    # --- managed backups ---

    async def backup_now(self) -> None:
        """``POST /admin/backup`` → 202; one ``pg_dump`` runs in the background (async).

        See :meth:`RtDbAdminClient.backup_now` for the idempotent-guard semantics.
        """
        resp = await self._req("POST", "/admin/backup", json={})
        self._expect_ok(resp)

    async def list_backups(self) -> dict[str, Any]:
        """``GET /admin/backups`` → ``{running, backups:[...]}`` (async)."""
        resp = await self._req("GET", "/admin/backups")
        return dict(resp.json())

    async def download_backup(self, name: str) -> bytes:
        """``GET /admin/backups/{name}`` → the dump file's raw bytes (async)."""
        resp = await self._req("GET", f"/admin/backups/{name}")
        return resp.content

    async def delete_backup(self, name: str) -> None:
        """``DELETE /admin/backups/{name}`` → 204; removes one dump file (async)."""
        await self._req("DELETE", f"/admin/backups/{name}")

    async def restore_backup(self, name: str) -> dict[str, Any]:
        """``POST /admin/restore`` ``{name, confirm}`` → ``{target, instructions}`` (async).

        See :meth:`RtDbAdminClient.restore_backup` for the typed-confirm guard.
        """
        resp = await self._req(
            "POST",
            "/admin/restore",
            json={"name": name, "confirm": name},
        )
        return dict(resp.json())

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
        body: dict[str, Any] = {"db": db, "name": name, "readOnly": read_only}
        if expires_at is not None:
            body["expiresAt"] = expires_at
        if tables is not None:
            body["tables"] = list(tables)
        resp = await self._req("POST", "/admin/mint-token", json=body)
        return MintedToken.model_validate(resp.json())

    async def revoke_token(self, token_id: str) -> None:
        """``POST /admin/revoke-token`` ``{tokenId}`` → ``{ok:true}`` (async)."""
        resp = await self._req("POST", "/admin/revoke-token", json={"tokenId": token_id})
        self._expect_ok(resp)

    async def list_tokens(self, db: str) -> list[TokenInfo]:
        """``GET /admin/tokens?db=<db>`` → ``[TokenInfo, ...]`` (async)."""
        resp = await self._req("GET", "/admin/tokens", params={"db": db})
        return [TokenInfo.model_validate(t) for t in resp.json()["tokens"]]

    # --- webhook surface (ENH-003) ---

    async def list_webhooks(self, db: str) -> list[Webhook]:
        """``GET /admin/db/{db}/webhooks`` → ``{webhooks:[...]}`` (async).

        See :meth:`RtDbAdminClient.list_webhooks` for empty-list semantics.
        """
        resp = await self._req("GET", f"/admin/db/{db}/webhooks")
        return [Webhook.model_validate(w) for w in resp.json()["webhooks"]]

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
        body: dict[str, Any] = {"url": url}
        if table is not None:
            body["table"] = table
        if events is not None:
            body["events"] = list(events)
        if enabled is not None:
            body["enabled"] = enabled
        resp = await self._req("POST", f"/admin/db/{db}/webhooks", json=body)
        return int(resp.json()["id"])

    async def edit_webhook(
        self,
        db: str,
        id: int,
        *,
        url: str | None = None,
        table: str | None | object = _UNSET,
        events: list[str] | None = None,
        enabled: bool | None = None,
    ) -> Webhook:
        """``PUT /admin/db/{db}/webhooks/{id}`` → updated :class:`Webhook` (async).

        See :meth:`RtDbAdminClient.edit_webhook` for the ``table`` tri-state
        (omitted vs ``None`` vs string) and body-building rules.
        """
        body: dict[str, Any] = {}
        if url is not None:
            body["url"] = url
        if table is not _UNSET:
            body["table"] = table
        if events is not None:
            body["events"] = list(events)
        if enabled is not None:
            body["enabled"] = enabled
        resp = await self._req("PUT", f"/admin/db/{db}/webhooks/{id}", json=body)
        return Webhook.model_validate(resp.json())

    async def delete_webhook(self, db: str, id: int) -> None:
        """``DELETE /admin/db/{db}/webhooks/{id}`` → ``{ok:true}`` (async)."""
        resp = await self._req("DELETE", f"/admin/db/{db}/webhooks/{id}")
        self._expect_ok(resp)

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
        params: dict[str, Any] = {}
        if status is not None:
            params["status"] = status
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        resp = await self._req(
            "GET",
            f"/admin/db/{db}/webhooks/{id}/deliveries",
            params=params,
        )
        return [WebhookDelivery.model_validate(d) for d in resp.json()["deliveries"]]

    # --- request plumbing ---

    async def _req(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Issue a request; raise :class:`RtDbError` on non-2xx, else return it."""
        resp = await self._client.request(method, path, **kwargs)
        if not resp.is_success:
            raise RtDbError.from_http(resp.status_code, resp.content)
        return resp

    @staticmethod
    def _expect_ok(resp: httpx.Response) -> None:
        """Assert the body is ``{ok: true}``; raise :class:`RtDbError` otherwise."""
        if not resp.json().get("ok"):
            raise RtDbError(ErrorCode.INTERNAL, "admin request returned ok=false")
