"""Schema DSL: ``FieldType`` (15 variants), ``IndexDef``, ``VectorIndexSpec``,
``TableDef``, ``SchemaDef``, fluent builders, and the ``t`` field-constructor
namespace.

Mirrors ``server/src/schema.rs`` field-for-field (the wire contract is
``#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]``) and
cross-checked against ``rust-client/src/schema.rs`` and
``ts-client/src/schema.ts``. The 15 ``FieldType`` variants:

* scalars (``{type}`` only): ``string``/``number``/``boolean``/``null``/
  ``any``/``int64``/``bytes``
* compound (``{type, <one payload field>}``): ``id{table}``/``literal{value}``/
  ``optional{inner}``/``union{variants}``/``array{element}``/
  ``object{fields}``/``record{value}``/``vector{dimensions}``

Wire notes:
* ``int64`` is a JSON string (i64 range too wide for JS/JSON number); the SDK
  treats it opaquely as ``str``.
* ``Id<Table>`` is likewise a typed ``str`` branded at the type layer only.
* ``IndexDef.search``/``IndexDef.vector`` and ``TableDef.owner_field`` are
  omitted from the wire when absent (server uses
  ``skip_serializing_if = "Option::is_none"`` / ``is_false`` / ``Vec::is_empty``).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_serializer

from .wire import to_camel


class _S(BaseModel):
    """Base for schema models: camelCase wire keys, reject unknown fields."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        alias_generator=to_camel,
    )


# --- FieldType (discriminator "type"; extra forbidden on every variant) ---


class _FString(_S):
    type: Literal["string"] = "string"


class _FNumber(_S):
    type: Literal["number"] = "number"


class _FBoolean(_S):
    type: Literal["boolean"] = "boolean"


class _FNull(_S):
    type: Literal["null"] = "null"


class _FId(_S):
    type: Literal["id"] = "id"
    table: str


class _FLiteral(_S):
    type: Literal["literal"] = "literal"
    value: Any


class _FOptional(_S):
    type: Literal["optional"] = "optional"
    inner: FieldType


class _FUnion(_S):
    type: Literal["union"] = "union"
    variants: list[FieldType]


class _FArray(_S):
    type: Literal["array"] = "array"
    element: FieldType


class _FObject(_S):
    type: Literal["object"] = "object"
    fields: dict[str, FieldType]


class _FInt64(_S):
    type: Literal["int64"] = "int64"


class _FBytes(_S):
    type: Literal["bytes"] = "bytes"


class _FAny(_S):
    type: Literal["any"] = "any"


class _FRecord(_S):
    type: Literal["record"] = "record"
    value: FieldType


class _FVector(_S):
    type: Literal["vector"] = "vector"
    dimensions: int


FieldType = Annotated[
    (
        _FString
        | _FNumber
        | _FBoolean
        | _FNull
        | _FId
        | _FLiteral
        | _FOptional
        | _FUnion
        | _FArray
        | _FObject
        | _FInt64
        | _FBytes
        | _FAny
        | _FRecord
        | _FVector
    ),
    Field(discriminator="type"),
]


# --- Indexes ---


class VectorIndexSpec(_S):
    """Vector-index spec carried on ``IndexDef.vector``.

    ``filter_fields`` serializes as ``filterFields`` and is omitted on the wire
    when empty (mirrors server's ``Vec::is_empty`` skip rule).
    """

    dimensions: int
    filter_fields: list[str] = Field(default_factory=list)

    @model_serializer(mode="wrap")
    def _drop_empty_filter_fields(self, handler):  # type: ignore[no-untyped-def]
        out = handler(self)
        if not out.get("filterFields"):
            out.pop("filterFields", None)
        return out


class IndexDef(_S):
    """Index declaration: btree, full-text search (``search=True``), or vector
    (``vector=...``). ``search``/``vector`` are omitted on the wire when absent,
    so a plain btree index serializes as ``{"name", "fields"}`` only."""

    name: str
    fields: list[str]
    search: bool | None = None
    vector: VectorIndexSpec | None = None

    @model_serializer(mode="wrap")
    def _drop_absent_flags(self, handler):  # type: ignore[no-untyped-def]
        out = handler(self)
        if out.get("search") is None:
            out.pop("search", None)
        if out.get("vector") is None:
            out.pop("vector", None)
        return out


class TableDef(_S):
    """One table: typed fields, indexes, optional per-row ``owner_field``."""

    fields: dict[str, FieldType]
    indexes: list[IndexDef] = Field(default_factory=list)
    owner_field: str | None = None

    @model_serializer(mode="wrap")
    def _drop_absent_owner(self, handler):  # type: ignore[no-untyped-def]
        out = handler(self)
        if out.get("ownerField") is None:
            out.pop("ownerField", None)
        return out


class SchemaDef(_S):
    """A whole schema: tables keyed by name."""

    tables: dict[str, TableDef]


# Rebuild models that carry the recursive ``FieldType`` forward reference so
# pydantic resolves the alias against module globals now that it exists.
for _cls in (_FOptional, _FUnion, _FArray, _FObject, _FRecord, TableDef, SchemaDef):
    _cls.model_rebuild()


# --- Builders ---


class TableBuilder:
    """Fluent builder for one table's fields and indexes."""

    def __init__(self) -> None:
        self._fields: dict[str, Any] = {}
        self._indexes: list[dict[str, Any]] = []
        self._owner: str | None = None

    def field(self, name: str, ft: Any) -> TableBuilder:
        self._fields[name] = ft
        return self

    def index(self, name: str, fields: list[str]) -> TableBuilder:
        self._indexes.append({"name": name, "fields": fields})
        return self

    def search_index(self, name: str, fields: list[str]) -> TableBuilder:
        self._indexes.append({"name": name, "fields": fields, "search": True})
        return self

    def vector_index(
        self,
        name: str,
        field: str,
        dimensions: int,
        *,
        filter_fields: list[str] | None = None,
    ) -> TableBuilder:
        spec: dict[str, Any] = {"dimensions": dimensions}
        if filter_fields:
            spec["filterFields"] = list(filter_fields)
        self._indexes.append(
            {"name": name, "fields": [field], "vector": spec}
        )
        return self

    def owner_field(self, name: str) -> TableBuilder:
        self._owner = name
        return self

    def _build(self) -> dict[str, Any]:
        out: dict[str, Any] = {"fields": self._fields, "indexes": self._indexes}
        if self._owner is not None:
            out["ownerField"] = self._owner
        return out


class SchemaBuilder:
    """Fluent builder for a whole schema."""

    def __init__(self) -> None:
        self._tables: dict[str, dict[str, Any]] = {}

    def table(self, name: str, configure: Any) -> SchemaBuilder:
        tb = TableBuilder()
        configure(tb)
        self._tables[name] = tb._build()
        return self

    def build(self) -> SchemaDef:
        return SchemaDef.model_validate({"tables": self._tables})


# --- ``t`` field constructors and ``Schema`` entry point ---
#
# Constructors return the wire-identical dict (the simplest form); ``build()``
# validates them into ``SchemaDef`` via ``model_validate``, which routes each
# field through the discriminated ``FieldType`` union. ``t._validate`` is a
# thin affordance for tests that want to validate a single ``FieldType``.


_field_type_adapter = TypeAdapter(FieldType)


class _SchemaNamespace:
    """Field-type constructors (``t.string()`` ... ``t.vector(n)``)."""

    @staticmethod
    def _validate(v: Any) -> Any:
        return _field_type_adapter.validate_python(v)

    string = staticmethod(lambda: {"type": "string"})
    number = staticmethod(lambda: {"type": "number"})
    boolean = staticmethod(lambda: {"type": "boolean"})
    null = staticmethod(lambda: {"type": "null"})
    any = staticmethod(lambda: {"type": "any"})
    int64 = staticmethod(lambda: {"type": "int64"})
    bytes = staticmethod(lambda: {"type": "bytes"})

    @staticmethod
    def id(table: str) -> dict[str, Any]:
        return {"type": "id", "table": table}

    @staticmethod
    def literal(value: Any) -> dict[str, Any]:
        return {"type": "literal", "value": value}

    @staticmethod
    def optional(inner: Any) -> dict[str, Any]:
        return {"type": "optional", "inner": inner}

    @staticmethod
    def union(variants: list[Any]) -> dict[str, Any]:
        return {"type": "union", "variants": variants}

    @staticmethod
    def array(element: Any) -> dict[str, Any]:
        return {"type": "array", "element": element}

    @staticmethod
    def object(fields: dict[str, Any]) -> dict[str, Any]:
        return {"type": "object", "fields": fields}

    @staticmethod
    def record(value: Any) -> dict[str, Any]:
        return {"type": "record", "value": value}

    @staticmethod
    def vector(dimensions: int) -> dict[str, Any]:
        return {"type": "vector", "dimensions": dimensions}


t = _SchemaNamespace()

Schema = SimpleNamespace(
    builder=SchemaBuilder,
    model_validate=SchemaDef.model_validate,
)
