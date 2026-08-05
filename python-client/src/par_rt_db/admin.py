"""Admin control-plane client for par-rt-db.

Dedicated admin-key bearer client for the ``/admin/*`` surface. This module
bootstraps the token management surface (ENH-005): mint/revoke/list with the
capability fields (``expiresAt``, ``readOnly``, ``tables``). The remaining
admin methods (db lifecycle, schema push, backup, allowlist, metrics, etc.)
still live on :class:`par_rt_db.http_client.RtDbHttpClient` /
:class:`par_rt_db.aio_http_client.RtDbAsyncHttpClient` today and migrate here
in a follow-up parity sweep — the constructor + ``_req`` helper + three token
methods below lay the foundation they will extend.

Two classes mirror the sync/async split of the data-plane HTTP client:
:class:`RtDbAdminClient` (sync, :class:`httpx.Client`) and
:class:`AsyncRtDbAdminClient` (async, :class:`httpx.AsyncClient`). ``httpx``
is imported lazily inside ``__init__`` so this module imports without the
``[http]``/``[aio]`` extra installed; the error surfaces only when a caller
actually constructs a client without httpx available.

Wire contract (byte-identical with the server, ``server/src/admin.rs``):

* ``POST /admin/mint-token`` ``{db, name, expiresAt?, readOnly, tables?}`` →
  ``{tokenId, token}``.
* ``POST /admin/revoke-token`` ``{tokenId}`` → ``{ok: true}``.
* ``GET /admin/tokens?db=<db>`` → ``{tokens: [{id, name, createdAt, revoked,
  expiresAt: int|null, readOnly: bool, tables: string[]|null}]}``.

The on-the-wire keys are camelCase; Python attributes are snake_case with an
explicit ``from_dict`` mapping on each dataclass.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .errors import ErrorCode, RtDbError

if TYPE_CHECKING:
    import httpx


@dataclass
class MintedToken:
    """``POST /admin/mint-token`` response: ``{tokenId, token}``.

    Wire camelCase (``tokenId``) maps to the snake_case attribute
    (:attr:`token_id`) via :meth:`from_dict`.
    """

    token_id: str
    token: str

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> MintedToken:
        """Build from the wire ``{tokenId, token}`` dict."""
        return cls(token_id=raw["tokenId"], token=raw["token"])


@dataclass
class TokenInfo:
    """One row of ``GET /admin/tokens`` — a token's metadata.

    ``expires_at``/``tables`` are ``None`` for a full-access token (the server
    serializes them as JSON ``null``); ``read_only`` is ``False`` for a
    read-write token. Mirrors ``server::admin::TokenRow`` with
    ``#[serde(rename_all = "camelCase")]``.
    """

    id: str
    name: str
    created_at: int
    revoked: bool
    expires_at: int | None = None
    read_only: bool = False
    tables: list[str] | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> TokenInfo:
        """Build from a wire row.

        ``expiresAt``/``readOnly``/``tables`` use ``dict.get`` defaults so an
        older server that omits them still deserializes (matches the server's
        ``#[serde(default)]`` on the corresponding ``TokenRow`` columns).
        """
        return cls(
            id=raw["id"],
            name=raw["name"],
            created_at=raw["createdAt"],
            revoked=raw["revoked"],
            expires_at=raw.get("expiresAt"),
            read_only=raw.get("readOnly", False),
            tables=raw.get("tables"),
        )


class RtDbAdminClient:
    """Sync admin control-plane client (the ``[http]`` extra).

    Authenticates every call with the instance admin key (bearer). Construct
    with the admin key and use as a context manager to close the underlying
    :class:`httpx.Client`::

        with RtDbAdminClient(url, admin_key) as c:
            minted = c.mint_token("mydb", "scraper", read_only=True, tables=["users"])

    Token surface (ENH-005): :meth:`mint_token`, :meth:`revoke_token`,
    :meth:`list_tokens`. The remaining admin methods arrive in a follow-up
    parity sweep.
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
        return MintedToken.from_dict(resp.json())

    def revoke_token(self, token_id: str) -> None:
        """``POST /admin/revoke-token`` ``{tokenId}`` → ``{ok:true}``."""
        resp = self._req("POST", "/admin/revoke-token", json={"tokenId": token_id})
        self._expect_ok(resp)

    def list_tokens(self, db: str) -> list[TokenInfo]:
        """``GET /admin/tokens?db=<db>`` → ``[TokenInfo, ...]``."""
        resp = self._req("GET", "/admin/tokens", params={"db": db})
        return [TokenInfo.from_dict(t) for t in resp.json()["tokens"]]

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
        return MintedToken.from_dict(resp.json())

    async def revoke_token(self, token_id: str) -> None:
        """``POST /admin/revoke-token`` ``{tokenId}`` → ``{ok:true}`` (async)."""
        resp = await self._req("POST", "/admin/revoke-token", json={"tokenId": token_id})
        self._expect_ok(resp)

    async def list_tokens(self, db: str) -> list[TokenInfo]:
        """``GET /admin/tokens?db=<db>`` → ``[TokenInfo, ...]`` (async)."""
        resp = await self._req("GET", "/admin/tokens", params={"db": db})
        return [TokenInfo.from_dict(t) for t in resp.json()["tokens"]]

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
