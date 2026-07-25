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
  ``#[serde(rename = "vectorSearch")]``); its ``filter`` is an eq-map
  ``dict[str, Any]`` over the index's declared ``filterFields`` (NOT a
  ``FilterExpr``), omitted when ``None`` or empty.

``QueryResult`` arrives untagged (the server's ``#[serde(untagged)]``); this
module re-attaches a shape via the terminal (``get``/``collect``/``first``/
``unique``/``count``/``paginate``) that produced it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_serializer

from .wire import FilterExpr, SearchQuery, VectorSearchQuery, to_camel


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
    def _drop_none_cursor(self, handler):  # type: ignore[no-untyped-def]
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
    filter: FilterExpr | None = None
    search: SearchQuery | None = None
    vector_search: VectorSearchQuery | None = Field(default=None, alias="vectorSearch")
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
        self._get: str | None = None
        self._filter: FilterExpr | None = None
        self._search: SearchQuery | None = None
        self._vector: VectorSearchQuery | None = None
        self._paginate: _Paginate | None = None

    # --- builder methods (return self) ---

    def get(self, id_: str) -> TableQuery:
        self._get = id_
        return self

    def with_index(self, index: str) -> TableQuery:
        self._index = index
        return self

    def eq(self, *values: Any) -> TableQuery:
        self._eq = list(values)
        return self

    def gt(self, v: Any) -> TableQuery:
        self._gt = v
        return self

    def gte(self, v: Any) -> TableQuery:
        self._gte = v
        return self

    def lt(self, v: Any) -> TableQuery:
        self._lt = v
        return self

    def lte(self, v: Any) -> TableQuery:
        self._lte = v
        return self

    def order(self, direction: Literal["asc", "desc"]) -> TableQuery:
        self._order = direction
        return self

    def take(self, n: int) -> TableQuery:
        self._take = n
        return self

    def filter(self, f: FilterExpr) -> TableQuery:
        self._filter = f
        return self

    def search(self, index: str, query: str) -> TableQuery:
        self._search = SearchQuery.model_validate({"index": index, "query": query})
        return self

    def vector_search(
        self,
        index: str,
        vector: list[float],
        *,
        limit: int,
        filter_: dict[str, Any] | None = None,
    ) -> TableQuery:
        """Vector-similarity terminal. ``filter_`` is an eq-map over the index's
        declared ``filterFields`` (NOT a ``FilterExpr``); omitted when ``None``
        or empty (mirrors the server's ``BTreeMap::is_empty`` skip rule)."""
        payload: dict[str, Any] = {"index": index, "vector": vector, "limit": limit}
        if filter_:
            payload["filter"] = filter_
        self._vector = VectorSearchQuery.model_validate(payload)
        return self

    # --- terminals ---

    def collect(self) -> TableQuery:
        return self

    def unique(self) -> TableQuery:
        self._unique = True
        return self

    def first(self) -> TableQuery:
        self._first = True
        return self

    def count(self) -> TableQuery:
        self._count = True
        return self

    def paginate(self, *, cursor: str | None = None, num_items: int) -> TableQuery:
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
            or self._paginate is not None
        ):
            raise ValueError("get is mutually exclusive with take/unique/first/count/paginate")
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
        if self._filter is not None:
            payload["filter"] = self._filter
        if self._search is not None:
            payload["search"] = self._search
        if self._vector is not None:
            payload["vectorSearch"] = self._vector
        if self._paginate is not None:
            payload["paginate"] = self._paginate
        return Query.model_validate(payload)

    # test affordances mirroring rust's typed terminals
    def build_for_count(self) -> Query:
        self._count = True
        return self.build()

    def build_for_first(self) -> Query:
        self._first = True
        return self.build()

    def build_for_unique(self) -> Query:
        self._unique = True
        return self.build()


def parse_result(model: type, terminal: str, value: Any) -> Any:
    """Deserialize an untagged ``QueryResult`` by the terminal that produced it.

    Args:
        model: A Pydantic ``BaseModel`` subclass to validate each doc against,
            or ``dict`` to return raw dicts (no per-doc validation).
        terminal: One of ``get``/``collect``/``first``/``unique``/``count``/
            ``paginate``.
        value: The raw ``QueryResult`` payload from the server.

    Returns:
        ``get``/``first``/``unique`` → ``model | None``;
        ``collect`` → ``list[model]``;
        ``count`` → ``int``;
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
