"""par-rt-db Python client.

The fourth implementation of par-rt-db's JSON wire contract, alongside
``server/src/protocol.rs``, ``ts-client/src/protocol.ts``, and
``rust-client/src/wire.rs``. This module packages the wire types and the
declarative schema/query/mutation DSL; HTTP, reactive WebSocket, and admin
clients land in follow-on plans (see
``docs/superpowers/specs/2026-07-25-python-client-design.md``).

Importing :mod:`par_rt_db` exposes the public DSL surface so
``from par_rt_db import Mutation, TableQuery, t`` works without per-module
imports. Wire-protocol types (``ClientMessage``/``ServerMessage``/``Schedule*``)
remain accessible via :mod:`par_rt_db.wire`; only the DSL symbols needed to
build queries, transactions, schemas, and cursors are re-exported here.
"""

from __future__ import annotations

from .cursor import decode_cursor, encode_cursor
from .errors import ErrorCode, RtDbError
from .mutation import Mutation, StepResult, Transaction
from .query import Paginated, Query, TableQuery
from .schema import SchemaDef, TableDef, t
from .wire import FilterExpr

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
]
