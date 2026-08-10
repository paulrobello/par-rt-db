"""par-rt-db Python client.

The fourth implementation of par-rt-db's JSON wire contract, alongside
``server/src/protocol.rs``, ``ts-client/src/protocol.ts``, and
``rust-client/src/wire.rs``. This module packages the wire types and the
declarative schema/query/mutation DSL; the one-shot HTTP client (data plane,
storage, admin control plane) ships here (``[http]`` extra), its async twin
(``[aio]`` extra), and the reactive WebSocket client (``[ws]`` extra — see
``docs/superpowers/specs/2026-07-25-python-client-design.md``).

Importing :mod:`par_rt_db` exposes the public DSL surface so
``from par_rt_db import Mutation, TableQuery, t`` works without per-module
imports. Wire-protocol types (``ClientMessage``/``ServerMessage``/``Schedule*``)
remain accessible via :mod:`par_rt_db.wire`; only the DSL symbols needed to
build queries, transactions, schemas, and cursors are re-exported here.

The sync HTTP client (``RtDbHttpClient``), its async twin
(``RtDbAsyncHttpClient``), and the reactive WebSocket client (``RtDbClient`` /
``Subscription``) are re-exported below via a lazy ``__getattr__`` so that
importing :mod:`par_rt_db` does NOT require ``httpx`` or ``websockets`` — the
``[http]`` / ``[aio]`` / ``[ws]`` extras are only needed when the relevant
client is actually constructed. Importing the package without any extra
continues to work for the wire/DSL surface.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .cursor import decode_cursor, encode_cursor
from .errors import ErrorCode, RtDbError
from .migration import Cast, Migration
from .mutation import Mutation, StepResult, Transaction
from .query import Paginated, Query, TableQuery
from .schema import SchemaDef, TableDef, t
from .wire import FilterExpr

if TYPE_CHECKING:
    from .admin import (
        AsyncRtDbAdminClient,
        AuditEntry,
        MintedToken,
        RtDbAdminClient,
        SchemaHistoryEntry,
        SchemaHistorySummary,
        SessionInfo,
        TokenInfo,
        Webhook,
        WebhookDelivery,
    )
    from .aio_http_client import RtDbAsyncHttpClient
    from .http_client import RtDbHttpClient
    from .ws_client import Presence, RtDbClient, Subscription

__all__ = [
    "Mutation",
    "Migration",
    "Cast",
    "Transaction",
    "StepResult",
    "SchemaDef",
    "TableDef",
    "t",
    "Query",
    "TableQuery",
    "Paginated",
    "FilterExpr",
    "encode_cursor",
    "decode_cursor",
    "RtDbError",
    "ErrorCode",
    "RtDbAsyncHttpClient",
    "RtDbHttpClient",
    "RtDbClient",
    "Subscription",
    "Presence",
    "RtDbAdminClient",
    "AsyncRtDbAdminClient",
    "MintedToken",
    "TokenInfo",
    "SessionInfo",
    "Webhook",
    "WebhookDelivery",
    "AuditEntry",
    "SchemaHistorySummary",
    "SchemaHistoryEntry",
]


def __getattr__(name: str) -> Any:
    """Lazy-load the optional-dep clients so ``httpx`` (``[http]``/``[aio]``)
    and ``websockets`` (``[ws]``) are only required when a client is actually
    used. Importing the package without any extra continues to work for the
    wire/DSL surface.
    """
    if name == "RtDbAsyncHttpClient":
        from . import aio_http_client

        return aio_http_client.RtDbAsyncHttpClient
    if name == "RtDbHttpClient":
        from . import http_client

        return http_client.RtDbHttpClient
    if name in ("RtDbClient", "Subscription", "Presence"):
        from . import ws_client

        return getattr(ws_client, name)
    if name in (
        "RtDbAdminClient",
        "AsyncRtDbAdminClient",
        "MintedToken",
        "TokenInfo",
        "SessionInfo",
        "Webhook",
        "WebhookDelivery",
        "AuditEntry",
        "SchemaHistorySummary",
        "SchemaHistoryEntry",
    ):
        from . import admin

        return getattr(admin, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
