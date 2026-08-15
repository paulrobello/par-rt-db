"""Query DSL: wire ``Query`` model, ``TableQuery`` builder, and ``QueryResult`` parsing.

Mirrors ``server/src/query.rs`` (the ``Query`` struct + ``QueryResult`` shapes)
and the builder ergonomics of ``ts-client/src/query.ts`` / ``rust-client/src/query.rs``.

Wire shapes (load-bearing — match the server exactly):

* ``Query`` fields are snake_case on the wire (NOT camelCase like most other
  models) and every field except ``table`` is optional; absent fields are
  omitted from the serialized payload, never emitted as ``null``.
* ``Query.paginate`` serializes as ``{cursor?, numItems}`` with ``cursor``
  omitted when ``None``.
* ``Query.vectorSearch`` is the one camelCase key on ``Query`` (the server uses
  ``#[serde(rename = "vectorSearch")]``); its ``filter`` is the full
  ``FilterExpr`` (the same type ``.filter()`` and ``search`` use), omitted when
  ``None``.

``QueryResult`` arrives untagged (the server's ``#[serde(untagged)]``); this
module re-attaches a shape via the terminal (``get``/``collect``/``first``/
``unique``/``count``/``distinct``/``aggregate``/``aggregateGroups``/``paginate``)
that produced it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_serializer
from pydantic_core.core_schema import SerializerFunctionWrapHandler

from .wire import (
    AggregateGroup,
    AggregateSpec,
    FilterExpr,
    HybridSearchQuery,
    SearchMode,
    SearchQuery,
    VectorSearchQuery,
    to_camel,
)


class _Paginate(BaseModel):
    """``Paginate`` block: ``{cursor?, numItems}``. ``cursor`` omitted when ``None``."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        alias_generator=to_camel,
    )

    cursor: str | None = None
    num_items: int

    @model_serializer(mode="wrap")
    def _drop_none_cursor(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("cursor") is None:
            out.pop("cursor", None)
        return out


class Query(BaseModel):
    """A read query. Wire field names are snake_case; all fields optional.

    ``vector_search`` serializes as ``vectorSearch`` (the lone camelCase key,
    matching the server's ``#[serde(rename = "vectorSearch")]``). ``None`` fields
    are dropped on serialization to match the server's omit-when-absent shape.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    table: str
    get: str | None = None
    index: str | None = None
    eq: list[Any] | None = None
    gt: Any | None = None
    gte: Any | None = None
    lt: Any | None = None
    lte: Any | None = None
    order: Literal["asc", "desc"] | None = None
    take: int | None = None
    unique: bool | None = None
    first: bool | None = None
    count: bool | None = None
    distinct: bool | None = None
    aggregate: AggregateSpec | None = None
    filter: FilterExpr | None = None
    search: SearchQuery | None = None
    vector_search: VectorSearchQuery | None = Field(default=None, alias="vectorSearch")
    hybrid_search: HybridSearchQuery | None = Field(default=None, alias="hybridSearch")
    paginate: _Paginate | None = None

    def model_dump(self, **kw: Any) -> dict[str, Any]:  # type: ignore[override]
        out = super().model_dump(**kw)
        # Drop Nones to match the server's all-optional, omit-when-absent shape.
        return {k: v for k, v in out.items() if v is not None}


@dataclass
class Paginated[T]:
    """A page of results: docs + an opaque next-page cursor (``None`` when exhausted)."""

    docs: list[T]
    next_cursor: str | None


class TableQuery:
    """Fluent builder producing a wire ``Query``.

    Terminal-aware: ``get`` is mutually exclusive with every other terminal
    (``take``/``unique``/``first``/``count``/``paginate``); the build step
    rejects a mixed configuration with ``ValueError`` so the error surfaces at
    build time rather than as a server-side ``BadRequest``.
    """

    def __init__(self, table: str) -> None:
        self._table = table
        self._index: str | None = None
        self._eq: list[Any] | None = None
        self._gt: Any = None
        self._gte: Any = None
        self._lt: Any = None
        self._lte: Any = None
        self._order: Literal["asc", "desc"] | None = None
        self._take: int | None = None
        self._unique: bool = False
        self._first: bool = False
        self._count: bool = False
        self._distinct: bool = False
        self._aggregate: AggregateSpec | None = None
        self._get: str | None = None
        self._filter: FilterExpr | None = None
        self._search: SearchQuery | None = None
        self._vector: VectorSearchQuery | None = None
        self._hybrid: HybridSearchQuery | None = None
        self._paginate: _Paginate | None = None

    # --- builder methods (return self) ---

    def get(self, id_: str) -> TableQuery:
        """Point-read terminal: read a single document by id. Mutually exclusive
        with every other terminal (enforced at build time)."""
        self._get = id_
        return self

    def with_index(self, index: str) -> TableQuery:
        """Select the index to scan; the eq prefix and range bounds apply to its
        fields in declared order."""
        self._index = index
        return self

    def eq(self, *values: Any) -> TableQuery:
        """Equality prefix on the index fields, one value per field in declared order."""
        self._eq = list(values)
        return self

    def gt(self, v: Any) -> TableQuery:
        """Strict-greater-than range bound on the next index field after the eq prefix."""
        self._gt = v
        return self

    def gte(self, v: Any) -> TableQuery:
        """Greater-than-or-equal range bound on the next index field after the eq prefix."""
        self._gte = v
        return self

    def lt(self, v: Any) -> TableQuery:
        """Strict-less-than range bound on the next index field after the eq prefix."""
        self._lt = v
        return self

    def lte(self, v: Any) -> TableQuery:
        """Less-than-or-equal range bound on the next index field after the eq prefix."""
        self._lte = v
        return self

    def order(self, direction: Literal["asc", "desc"]) -> TableQuery:
        """Sort direction (``"asc"`` or ``"desc"``) for the ordered terminals
        (``take``/``first``/``paginate``)."""
        self._order = direction
        return self

    def take(self, n: int) -> TableQuery:
        """Cap results to the first ``n`` matching rows (``collect``/``unique`` terminal)."""
        self._take = n
        return self

    def filter(self, f: FilterExpr) -> TableQuery:
        """Apply a ``FilterExpr`` predicate over document fields. Mutually exclusive
        with ``search``/``vector_search``/``hybrid_search``."""
        self._filter = f
        return self

    def search(
        self,
        index: str,
        query: str,
        *,
        filter_: FilterExpr | None = None,
        mode: SearchMode | None = None,
    ) -> TableQuery:
        """Full-text search terminal. ``filter_`` is a ``FilterExpr`` (the same
        type ``.filter()`` and ``authorize`` use) that narrows search results
        server-side; omitted when ``None``. The trailing underscore mirrors
        ``vector_search``'s ``filter_`` keyword. The nested search filter is
        distinct from the query-level ``.filter()`` builder (which is mutually
        exclusive with ``search``). ``mode`` selects the match strategy (FM-30):
        ``None`` (default) or ``"tsquery"`` is today's full-text behavior;
        ``"trgm"`` is case-insensitive substring/autocomplete matching over the
        index's text fields. Omitted from the wire when ``None`` so existing
        requests stay byte-identical."""
        payload: dict[str, Any] = {"index": index, "query": query}
        if filter_ is not None:
            payload["filter"] = filter_
        if mode is not None:
            payload["mode"] = mode
        self._search = SearchQuery.model_validate(payload)
        return self

    def vector_search(
        self,
        index: str,
        vector: list[float],
        *,
        limit: int,
        filter_: FilterExpr | None = None,
    ) -> TableQuery:
        """Vector-similarity terminal. ``filter_`` is a ``FilterExpr`` (the same
        type ``.filter()`` and ``search`` use) that narrows vector-search results
        server-side; omitted when ``None``. The trailing underscore mirrors
        ``search``'s ``filter_`` keyword."""
        payload: dict[str, Any] = {"index": index, "vector": vector, "limit": limit}
        if filter_ is not None:
            payload["filter"] = filter_
        self._vector = VectorSearchQuery.model_validate(payload)
        return self

    def hybrid_search(
        self,
        query: str,
        vector: list[float],
        *,
        limit: int,
        search_index: str | None = None,
        vector_index: str | None = None,
        k: int | None = None,
    ) -> TableQuery:
        """Hybrid search terminal: fuses full-text and vector ranking via
        Reciprocal Rank Fusion. The table must declare BOTH a search index and a
        vector index. ``search_index``/``vector_index`` optionally name the
        indexes (auto-selected server-side when ``None``); ``k`` is the RRF
        constant (default 60). Mutually exclusive with every other terminal."""
        payload: dict[str, Any] = {"query": query, "vector": vector, "limit": limit}
        if search_index is not None:
            payload["searchIndex"] = search_index
        if vector_index is not None:
            payload["vectorIndex"] = vector_index
        if k is not None:
            payload["k"] = k
        self._hybrid = HybridSearchQuery.model_validate(payload)
        return self

    # --- terminals ---

    def collect(self) -> TableQuery:
        """Collect terminal (the default): return all matching rows as a list."""
        return self

    def unique(self) -> TableQuery:
        """``unique`` terminal: de-duplicate matching rows by the index field value."""
        self._unique = True
        return self

    def first(self) -> TableQuery:
        """``first`` terminal: return the first matching row, or ``None``."""
        self._first = True
        return self

    def count(self) -> TableQuery:
        """``count`` terminal: return the number of matching rows."""
        self._count = True
        return self

    def distinct(self) -> TableQuery:
        """``distinct`` terminal: return the unique values of the index field
        after the eq prefix."""
        self._distinct = True
        return self

    def aggregate(
        self, op: Literal["sum", "avg", "min", "max", "count"], *, group_by: bool = False
    ) -> TableQuery:
        """Aggregate terminal: runs ``<op>`` (SUM/AVG/MIN/MAX/COUNT) over the
        index field after the eq prefix. With ``group_by=True``, groups by that
        field and aggregates the next one. ``sum``/``avg`` require a numeric
        aggregate field; ``count`` aggregates rows and consumes no aggregate
        field (a scalar ``count`` needs no index at all; a grouped ``count``
        needs one index field beyond the eq prefix to group by). The server
        rejects non-numeric, no-index, or no-field-beyond-prefix cases for the
        field-bearing ops. Mutually exclusive with every other terminal except
        ``eq``/range bounds/``filter``; ``take`` is also rejected — group count
        is capped internally by MAX_TAKE."""
        self._aggregate = AggregateSpec.model_validate({"op": op, "groupBy": bool(group_by)})
        return self

    def paginate(self, *, cursor: str | None = None, num_items: int) -> TableQuery:
        """``paginate`` terminal: return a page of ``num_items`` rows starting after
        the opaque ``cursor`` (``None`` for the first page). The result carries a
        ``next_cursor`` that is ``None`` once exhausted."""
        self._paginate = _Paginate.model_validate({"cursor": cursor, "numItems": num_items})
        return self

    # --- build ---

    def build(self) -> Query:
        """Materialize the wire ``Query``, enforcing terminal mutual-exclusion."""
        if self._get is not None and (
            self._take is not None
            or self._unique
            or self._first
            or self._count
            or self._distinct
            or self._aggregate is not None
            or self._paginate is not None
        ):
            raise ValueError(
                "get is mutually exclusive with take/unique/first/count/distinct/aggregate/paginate"
            )
        payload: dict[str, Any] = {"table": self._table}
        if self._get is not None:
            payload["get"] = self._get
        if self._index is not None:
            payload["index"] = self._index
        if self._eq is not None:
            payload["eq"] = self._eq
        if self._gt is not None:
            payload["gt"] = self._gt
        if self._gte is not None:
            payload["gte"] = self._gte
        if self._lt is not None:
            payload["lt"] = self._lt
        if self._lte is not None:
            payload["lte"] = self._lte
        if self._order is not None:
            payload["order"] = self._order
        if self._take is not None:
            payload["take"] = self._take
        if self._unique:
            payload["unique"] = True
        if self._first:
            payload["first"] = True
        if self._count:
            payload["count"] = True
        if self._distinct:
            payload["distinct"] = True
        if self._aggregate is not None:
            payload["aggregate"] = self._aggregate
        if self._filter is not None:
            payload["filter"] = self._filter
        if self._search is not None:
            payload["search"] = self._search
        if self._vector is not None:
            payload["vectorSearch"] = self._vector
        if self._hybrid is not None:
            payload["hybridSearch"] = self._hybrid
        if self._paginate is not None:
            payload["paginate"] = self._paginate
        return Query.model_validate(payload)

    # test affordances mirroring rust's typed terminals
    def build_for_count(self) -> Query:
        """Set the ``count`` terminal then build."""
        self._count = True
        return self.build()

    def build_for_first(self) -> Query:
        """Set the ``first`` terminal then build."""
        self._first = True
        return self.build()

    def build_for_unique(self) -> Query:
        """Set the ``unique`` terminal then build."""
        self._unique = True
        return self.build()

    def build_for_distinct(self) -> Query:
        """Set the ``distinct`` terminal then build."""
        self._distinct = True
        return self.build()

    def build_for_aggregate(
        self, op: Literal["sum", "avg", "min", "max", "count"], *, group_by: bool = False
    ) -> Query:
        """Set the ``aggregate`` terminal then build. See :meth:`aggregate` for
        op/grouping semantics."""
        self.aggregate(op, group_by=group_by)
        return self.build()


def _dump_query(q: Query | TableQuery) -> dict[str, Any]:
    """Serialize a Query (or TableQuery) to its wire-shaped dict."""
    built = q.build() if isinstance(q, TableQuery) else q
    return built.model_dump(by_alias=True, mode="json")


def _terminal_of(q: Query) -> str:
    """Infer the parse_result terminal from a built Query."""
    if q.get is not None:
        return "get"
    if q.count:
        return "count"
    if q.first:
        return "first"
    if q.unique:
        return "unique"
    if q.distinct:
        return "distinct"
    if q.aggregate is not None:
        return "aggregateGroups" if q.aggregate.group_by else "aggregate"
    if q.paginate is not None:
        return "paginate"
    return "collect"


def parse_result(model: type[Any], terminal: str, value: Any) -> Any:
    """Deserialize an untagged ``QueryResult`` by the terminal that produced it.

    Args:
        model: A Pydantic ``BaseModel`` subclass to validate each doc against,
            or ``dict`` to return raw dicts (no per-doc validation), or one of
            ``str``/``int``/``float``/``bool``/``Any`` for the scalar values
            returned by ``distinct``/``aggregate``.
        terminal: One of ``get``/``collect``/``first``/``unique``/``count``/
            ``distinct``/``aggregate``/``aggregateGroups``/``paginate``.
        value: The raw ``QueryResult`` payload from the server.

    Returns:
        ``get``/``first``/``unique`` → ``model | None``;
        ``collect`` → ``list[model]``;
        ``count`` → ``int``;
        ``distinct`` → ``list[model]`` (typically ``list[Any]`` — the unique
        scalar values of the index field after the eq prefix);
        ``aggregate`` → ``model | None`` (the scalar aggregate value, or ``None``
        when the server returned JSON null over an empty matching set; ``count``
        returns an integer and is never ``None`` — it yields ``0`` over an empty
        set);
        ``aggregateGroups`` → ``list[AggregateGroup]`` (the ``{key, value}``
        rows from a grouped aggregate);
        ``paginate`` → ``Paginated[model]``.

    Raises:
        ValueError: If ``terminal`` is not one of the known terminals.
    """
    if terminal == "get":
        return None if value is None else _coerce(model, value)
    if terminal == "collect":
        return [_coerce(model, v) for v in value]
    if terminal in ("first", "unique"):
        return None if value is None else _coerce(model, value)
    if terminal == "count":
        return int(value)
    if terminal == "distinct":
        return [_coerce(model, v) for v in value]
    if terminal == "aggregate":
        # Server returns a bare scalar (number/string/bool) or JSON null when no
        # rows match. Pass through `_coerce` so callers can request `float`,
        # `str`, or `Any` for the scalar shape.
        return None if value is None else _coerce(model, value)
    if terminal == "aggregateGroups":
        return [AggregateGroup.model_validate(v) for v in value]
    if terminal == "paginate":
        docs = [_coerce(model, v) for v in value.get("docs", [])]
        nxt = value.get("nextCursor")
        return Paginated(docs=docs, next_cursor=nxt)
    raise ValueError(f"unknown terminal: {terminal}")


def _coerce(model: type, value: Any) -> Any:
    """Validate ``value`` against ``model`` (``dict`` → raw dict, BaseModel
    subclass → ``model_validate``, anything else → ``TypeAdapter``)."""
    if model is dict:
        return dict(value)
    if isinstance(model, type) and issubclass(model, BaseModel):
        return model.model_validate(value)
    adapter = TypeAdapter(model)
    return adapter.validate_python(value)


# Resolve the ``_Paginate`` forward reference inside ``Query`` (annotations are
# deferred under ``from __future__ import annotations``).
Query.model_rebuild()
