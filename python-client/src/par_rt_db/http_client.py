"""One-shot HTTP client for par-rt-db. ``Authorization: Bearer <token>`` on every call.

The sync HTTP surface — the python port of ``rust-client/src/http.rs``'s
``RtDbHttpClient``. Mirrors the rust-client's data plane (``POST /api/query`` /
``POST /api/mutate``), storage (``POST /api/storage/{db}`` raw-body upload,
``DELETE``, ``GET .../metadata``, public serve URL), and admin control plane
(``POST /admin/...``, ``GET /admin/...``, ``POST /admin/db/{db}/...``). Routes,
request bodies, and response shapes are identical to the rust-client; only the
method names are snake_cased to match Python convention.

The reactive WebSocket client (``/sync``) is a separate async surface and lands
in a follow-on plan — this module is sync-only and depends only on ``httpx``.

``httpx`` is an optional dependency (``pip install par-rt-db[http]``); it is
imported lazily inside ``RtDbHttpClient.__init__`` so that importing this module
or ``par_rt_db`` without the ``[http]`` extra does not fail. The error surfaces
only when a caller actually constructs the client without httpx installed.

Token convention (same as rust-client): the bearer passed to the constructor is
sent on every call. For admin methods, construct the client with the instance
admin key as the token; for data-plane methods, construct it with a per-db
machine token (or OAuth session token).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, TypeAdapter

from .errors import ErrorCode, RtDbError
from .mutation import StepResult, Transaction
from .query import Query, TableQuery, _terminal_of, parse_result
from .schema import SchemaDef
from .wire import BatchQueryOutcome, ScheduleInfo, ScheduleWhen, to_camel

if TYPE_CHECKING:
    import httpx


class _Wire(BaseModel):
    """camelCase wire keys, reject unknown fields — for HTTP response models."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        alias_generator=to_camel,
    )


class MintedToken(_Wire):
    """``POST /admin/mint-token`` response: ``{tokenId, token}``."""

    token_id: str
    token: str


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


class AdminMember(_Wire):
    """``GET /admin/admins`` row — ``{email, githubId?}``."""

    email: str
    github_id: int | None = None


class TokenInfo(_Wire):
    """``GET /admin/tokens`` row — ``{id, name, createdAt, revoked}``."""

    id: str
    name: str
    created_at: int
    revoked: bool


class TableStat(_Wire):
    """One row of ``DbStats.tables`` — ``{name, rowCount, sizeBytes}``."""

    name: str
    row_count: int
    size_bytes: int


class DbStats(_Wire):
    """``GET /admin/dbs/{db}/stats`` response — per-table row counts + sizes."""

    tables: list[TableStat]
    total_size_bytes: int


class LatencyStats(_Wire):
    """p50/p95/p99 latency percentile triple (microseconds). Field names are
    already lowercase, so ``to_camel`` leaves them as ``p50``/``p95``/``p99``."""

    p50: int
    p95: int
    p99: int


class MetricsSnapshot(_Wire):
    """``GET /admin/metrics`` response — server-wide counters and gauges.

    The ``subs_*`` fields default to 0 so a client built against a newer server
    still deserializes an older server's response (these counters landed
    2026-07-29); 0 is the correct "not reported" value for a monotonic counter.
    """

    queries_total: int
    mutations_total: int
    uploads_total: int
    ws_connections: int
    active_subscriptions: int
    pool_size: int
    pool_idle: int
    uptime_seconds: int
    query_latency: LatencyStats
    mutate_latency: LatencyStats
    subscribe_latency: LatencyStats
    subs_reruns_total: int = 0
    subs_skips_point_total: int = 0
    subs_skips_indexed_total: int = 0
    subs_skips_ordered_total: int = 0
    subs_skip_verifications_total: int = 0
    subs_missed_pushes_total: int = 0


class HotConfig(_Wire):
    """Runtime-mutable hot config — mirrors ``server::config::HotConfig``."""

    allowed_origins: list[str]
    session_ttl_days: int
    max_file_size: int
    idempotency_ttl_ms: int


class ConfigResponse(_Wire):
    """``GET /admin/config`` response — redacted boot config + hot config + build
    identity + admin allowlist. Secrets surface as configured-bools, never values."""

    port: int
    public_url: str
    github_base_url: str
    github_api_url: str
    database_url_configured: bool
    admin_key_configured: bool
    github_configured: bool
    google_configured: bool
    gitlab_configured: bool
    hot: HotConfig
    version: str
    git_commit: str
    admins: list[AdminMember]


class HotConfigPatch(_Wire):
    """``PATCH /admin/config`` body — every field optional; omitted fields are
    left unchanged. Serialize with ``exclude_none=True`` so the wire body carries
    only the fields the caller sets (matches rust-client's
    ``skip_serializing_if = "Option::is_none"``). Unknown fields are rejected
    server-side (``deny_unknown_fields``).
    """

    allowed_origins: list[str] | None = None
    session_ttl_days: int | None = None
    max_file_size: int | None = None
    idempotency_ttl_ms: int | None = None


class OpEvent(_Wire):
    """``GET /admin/ops/recent`` row — one document-op event from the in-memory
    ring. ``kind`` is a lowercase string (``insert``/``patch``/``replace``/
    ``delete``/``upsert``); ``owner`` is ``null`` for admin/machine writes."""

    db: str
    table: str
    doc_id: str
    kind: str
    ts: int
    owner: str | None = None


# ``StepResult`` is a ``Union`` alias (no ``model_validate``); route through a
# single ``TypeAdapter`` for the untagged per-step result, mirroring mutation.py.
_STEP_RESULT_ADAPTER = TypeAdapter(StepResult)
# ``list[...]`` aliases likewise need a TypeAdapter to validate at runtime.
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

        ``when`` is a ``ScheduleWhen`` (``_AfterMs``/``_RunAt``/``_Cron`` from
        ``par_rt_db.wire``). One-shot jobs past due run immediately; cron jobs
        skip missed windows (server-side semantics).
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

    def upload(self, data: bytes, *, content_type: str | None = None) -> UploadResult:
        """``POST /api/storage/{db}`` with a raw body → server-computed metadata.

        ``content_type`` sets the ``Content-Type`` header AND is stored as the
        file's type; when ``None`` the header is left unset (httpx defaults to
        ``application/octet-stream``). Unlike every other method the body is NOT
        JSON.
        """
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

    def get_url(self, id: str) -> str:
        """The public serve URL (``GET /storage/{id}``) — no request is made."""
        return f"{self._base}/storage/{id}"

    # --- admin control plane (admin key as the token) ---

    def create_db(self, name: str) -> None:
        """``POST /admin/create-db`` ``{name}`` → ``{ok:true}``."""
        resp = self._send("POST", "/admin/create-db", json={"name": name})
        self._expect_ok(resp)

    def delete_db(self, name: str, confirm: str) -> None:
        """``POST /admin/delete-db`` ``{name, confirm}`` → ``{ok:true}``.

        The server rejects with ``BAD_REQUEST`` unless ``confirm == name``
        exactly — the typed confirmation guard against accidental deletion.
        """
        resp = self._send(
            "POST",
            "/admin/delete-db",
            json={"name": name, "confirm": confirm},
        )
        self._expect_ok(resp)

    def push_schema(self, db: str, schema: SchemaDef) -> None:
        """``POST /admin/push-schema`` ``{db, schema}`` → ``{ok:true}``."""
        resp = self._send(
            "POST",
            "/admin/push-schema",
            json={"db": db, "schema": schema.model_dump(by_alias=True, mode="json")},
        )
        self._expect_ok(resp)

    def list_dbs(self) -> list[str]:
        """``GET /admin/dbs`` → ``{databases:[...]}``."""
        resp = self._send("GET", "/admin/dbs")
        return list(resp.json()["databases"])

    def mint_token(self, db: str, name: str) -> MintedToken:
        """``POST /admin/mint-token`` ``{db, name}`` → ``{tokenId, token}``."""
        resp = self._send("POST", "/admin/mint-token", json={"db": db, "name": name})
        return MintedToken.model_validate(resp.json())

    def revoke_token(self, token_id: str) -> None:
        """``POST /admin/revoke-token`` ``{tokenId}`` → ``{ok:true}``."""
        resp = self._send("POST", "/admin/revoke-token", json={"tokenId": token_id})
        self._expect_ok(resp)

    def export_db(self, db: str) -> str:
        """``GET /admin/export-db?db=<db>`` → the database snapshot as JSONL text."""
        return self._send("GET", "/admin/export-db", params={"db": db}).text

    def import_db(self, db: str, jsonl: str) -> None:
        """``POST /admin/import-db?db=<db>`` with an ``application/x-ndjson`` body."""
        resp = self._send(
            "POST",
            "/admin/import-db",
            params={"db": db},
            content=jsonl,
            headers={"Content-Type": "application/x-ndjson"},
        )
        self._expect_ok(resp)

    def allowlist_add(self, db: str, email: str) -> None:
        """``POST /admin/allowlist`` ``{db, action:"add", email}`` → ``{ok:true}``."""
        resp = self._send(
            "POST",
            "/admin/allowlist",
            json={"db": db, "action": "add", "email": email},
        )
        self._expect_ok(resp)

    def allowlist_remove(self, db: str, email: str) -> None:
        """``POST /admin/allowlist`` ``{db, action:"remove", email}`` → ``{ok:true}``."""
        resp = self._send(
            "POST",
            "/admin/allowlist",
            json={"db": db, "action": "remove", "email": email},
        )
        self._expect_ok(resp)

    def allowlist_list(self, db: str) -> list[str]:
        """``GET /admin/allowlist?db=<db>`` → ``{emails:[...]}``."""
        resp = self._send("GET", "/admin/allowlist", params={"db": db})
        return list(resp.json()["emails"])

    def admins_list(self) -> list[AdminMember]:
        """``GET /admin/admins`` → ``{admins:[{email, githubId?}]}``."""
        resp = self._send("GET", "/admin/admins")
        return [AdminMember.model_validate(m) for m in resp.json()["admins"]]

    def admins_add(self, email: str, github_id: int | None = None) -> None:
        """``POST /admin/admins`` ``{email, githubId?}`` → ``{ok:true}``.

        ``githubId`` is omitted from the body when ``None`` (matches the
        server's ``skip_serializing_if`` rule).
        """
        body: dict[str, Any] = {"email": email}
        if github_id is not None:
            body["githubId"] = github_id
        resp = self._send("POST", "/admin/admins", json=body)
        self._expect_ok(resp)

    def admins_remove(self, email: str) -> None:
        """``DELETE /admin/admins`` ``{email}`` → ``{ok:true}``.

        Body-on-DELETE (axum reads it from the request body, not the URL) —
        mirrors the rust-client's ``delete_json``.
        """
        resp = self._send("DELETE", "/admin/admins", json={"email": email})
        self._expect_ok(resp)

    def list_tokens(self, db: str) -> list[TokenInfo]:
        """``GET /admin/tokens?db=<db>`` → ``{tokens:[{id,name,createdAt,revoked}]}``."""
        resp = self._send("GET", "/admin/tokens", params={"db": db})
        return [TokenInfo.model_validate(t) for t in resp.json()["tokens"]]

    def get_schema(self, db: str) -> SchemaDef:
        """``GET /admin/dbs/{db}/schema`` → the database's pushed ``SchemaDef``."""
        resp = self._send("GET", f"/admin/dbs/{db}/schema")
        return SchemaDef.model_validate(resp.json())

    def db_stats(self, db: str) -> DbStats:
        """``GET /admin/dbs/{db}/stats`` → per-table row counts + storage sizes."""
        resp = self._send("GET", f"/admin/dbs/{db}/stats")
        return DbStats.model_validate(resp.json())

    def metrics(self) -> MetricsSnapshot:
        """``GET /admin/metrics`` → server-wide counters and gauges."""
        resp = self._send("GET", "/admin/metrics")
        return MetricsSnapshot.model_validate(resp.json())

    def get_config(self) -> ConfigResponse:
        """``GET /admin/config`` → redacted running config + build identity + admins."""
        resp = self._send("GET", "/admin/config")
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
        resp = self._send("PATCH", "/admin/config", json=body)
        return ConfigResponse.model_validate(resp.json())

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
        resp = self._send("GET", "/admin/ops/recent", params=params)
        return [OpEvent.model_validate(e) for e in resp.json()["ops"]]

    # --- admin data access: owner-bypass query/mutate (admin key) ---

    def admin_query(self, db: str, query: Query | TableQuery, *, model: type = dict) -> Any:
        """``POST /admin/db/{db}/query`` ``{query}`` → parsed ``{result}``.

        Owner-bypass: an admin reads documents across every database regardless
        of ``ownerField``. ``db`` rides in the URL (singular ``db``), so the body
        omits it. Result parsing mirrors ``run``.
        """
        built = query.build() if isinstance(query, TableQuery) else query
        body = {"query": built.model_dump(by_alias=True, mode="json")}
        resp = self._send("POST", f"/admin/db/{db}/query", json=body)
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
        resp = self._send("POST", f"/admin/db/{db}/mutate", json=body)
        results = resp.json()["results"]
        return [_STEP_RESULT_ADAPTER.validate_python(r) for r in results]

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
