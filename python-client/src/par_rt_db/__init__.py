"""par-rt-db Python client.

The fourth implementation of par-rt-db's JSON wire contract, alongside
``server/src/protocol.rs``, ``ts-client/src/protocol.ts``, and
``rust-client/src/wire.rs``. This module packages the wire types and the
declarative schema/query/mutation DSL; the one-shot HTTP client (data plane,
storage, admin control plane) ships here, and the reactive WebSocket client
lands in a follow-on plan (see
``docs/superpowers/specs/2026-07-25-python-client-design.md``).

Importing :mod:`par_rt_db` exposes the public DSL surface so
``from par_rt_db import Mutation, TableQuery, t`` works without per-module
imports. Wire-protocol types (``ClientMessage``/``ServerMessage``/``Schedule*``)
remain accessible via :mod:`par_rt_db.wire`; only the DSL symbols needed to
build queries, transactions, schemas, and cursors are re-exported here.

The HTTP client (``RtDbHttpClient``) is re-exported below via a lazy
``__getattr__`` so that importing :mod:`par_rt_db` does NOT require ``httpx``
— the ``[http]`` extra is only needed when ``RtDbHttpClient`` is actually
constructed. Reactive WebSocket lands in a follow-on plan.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .cursor import decode_cursor, encode_cursor
from .errors import ErrorCode, RtDbError
from .mutation import Mutation, StepResult, Transaction
from .query import Paginated, Query, TableQuery
from .schema import SchemaDef, TableDef, t
from .wire import FilterExpr

if TYPE_CHECKING:
    from .http_client import RtDbHttpClient

__all__ = [
    "Mutation",
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
    "RtDbHttpClient",
]


def __getattr__(name: str) -> Any:
    """Lazy-load ``RtDbHttpClient`` so ``httpx`` (the ``[http]`` extra) is only
    required when the HTTP client is actually used. Importing the package
    without the extra continues to work for the wire/DSL surface.
    """
    if name == "RtDbHttpClient":
        from . import http_client

        return http_client.RtDbHttpClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
