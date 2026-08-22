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
from pydantic_core.core_schema import SerializerFunctionWrapHandler

from .value_expr import ValueExpr
from .wire import FilterExpr, to_camel


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


#: FM-33: app-level action fired when the referenced row is hard-declared
#: deleted via a ``Delete`` step — wire literals ``cascade`` | ``restrict`` |
#: ``setNull`` (server ``schema.rs::OnDeleteAction``, camelCase rename).
OnDeleteAction = Literal["cascade", "restrict", "setNull"]


class _FId(_S):
    type: Literal["id"] = "id"
    table: str
    # FM-33: optional `onDelete` action on the reference. Wire key `onDelete`,
    # omitted when unset (server `skip_serializing_if = "Option::is_none"`).
    on_delete: OnDeleteAction | None = None

    @model_serializer(mode="wrap")
    def _drop_absent_on_delete(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("onDelete") is None:
            out.pop("onDelete", None)
        return out


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
    when empty (mirrors server's ``Vec::is_empty`` skip rule). ``metric`` defaults
    to ``"cosine"`` and is omitted on the wire when ``"cosine"`` (mirrors the
    server's ``skip_serializing_if`` default-value rule, so existing schemas
    serialize byte-identically).
    """

    dimensions: int
    filter_fields: list[str] = Field(default_factory=list)
    metric: Literal["cosine", "l2", "ip"] = "cosine"

    @model_serializer(mode="wrap")
    def _drop_defaults(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if not out.get("filterFields"):
            out.pop("filterFields", None)
        if out.get("metric") == "cosine":
            out.pop("metric", None)
        return out


class IndexDef(_S):
    """Index declaration: btree, full-text search (``search=True``), or vector
    (``vector=...``). ``search``/``vector``/``unique``/``where``/``language``
    are omitted on the wire when absent, so a plain btree index serializes as
    ``{"name", "fields"}`` only.

    ``language`` optionally selects a Postgres ``regconfig`` name (e.g.
    ``"english"``, ``"simple"``, ``"spanish"``) for a search index's generated
    tsvector column and ``search``/``hybridSearch`` query-text parsing, so
    non-English corpora get correct stemming and stop-words. Valid only on a
    search index; the server default (field absent) behaves as ``english``.
    Mirrors ``server/src/schema.rs::IndexDef.language``
    (``skip_serializing_if = "Option::is_none"``). See ENH-006."""

    name: str
    fields: list[str]
    search: bool | None = None
    vector: VectorIndexSpec | None = None
    unique: bool | None = None
    where: FilterExpr | None = None
    language: str | None = None

    @model_serializer(mode="wrap")
    def _drop_absent_flags(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        # Server uses `skip_serializing_if = "is_false"` for `search: bool`, so
        # `search=False` (not just an absent/None flag) is omitted on the wire.
        if not out.get("search"):
            out.pop("search", None)
        if out.get("vector") is None:
            out.pop("vector", None)
        # Same falsy-drop for `unique: bool`; `where` is Option<FilterExpr> so it
        # drops only when None (a present predicate always serializes).
        if not out.get("unique"):
            out.pop("unique", None)
        if out.get("where") is None:
            out.pop("where", None)
        # `language` is Option<String>; drop only when None (a present value
        # always serializes).
        if out.get("language") is None:
            out.pop("language", None)
        return out


class TtlDef(_S):
    """Per-table TTL policy: a declared numeric ``field`` whose value is the
    document's expiry epoch-millis, plus an optional ``default_duration_ms``
    stamped onto inserts that omit the field (after insert it is an ordinary
    field). Mirrors ``server/src/schema.rs::TtlDef``; serializes as
    ``{"field": ..., "defaultDurationMs": ...}`` with ``defaultDurationMs`` omitted
    when ``None`` (server ``skip_serializing_if = "Option::is_none"``)."""

    field: str
    default_duration_ms: int | None = None

    @model_serializer(mode="wrap")
    def _drop_absent_default(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("defaultDurationMs") is None:
            out.pop("defaultDurationMs", None)
        return out


class TableDef(_S):
    """One table: typed fields, indexes, optional per-row ``owner_field`` and
    ``collaborators_field``, an optional ``ttl`` policy, an optional
    ``updated_at_field`` stamp policy (FM-36), an optional ``authorize``
    per-row predicate (Model C), optional field-level ``defaults`` (FM-32),
    and an optional ``soft_delete`` flag (FM-33)."""

    fields: dict[str, FieldType]
    indexes: list[IndexDef] = Field(default_factory=list)
    owner_field: str | None = None
    collaborators_field: str | None = None
    ttl: TtlDef | None = None
    # FM-36: names a declared number/int64 field the server stamps with the
    # current epoch-ms on every version-bumping write (insert, patch, replace,
    # upsert, patchByQuery, cascade setNull), overwriting any client-supplied
    # value. Wire key `updatedAtField`, omitted when unset (server
    # `skip_serializing_if = "Option::is_none"`).
    updated_at_field: str | None = None
    # FM-37: names a declared `int64` field the server stamps from a
    # per-table monotonic counter on insert (and upsert's insert branch),
    # overwriting any client-supplied value. Immutable after insert — a patch
    # or replace that changes the stored value is rejected. Wire key
    # `autoIncrementField`, omitted when unset (server
    # `skip_serializing_if = "Option::is_none"`).
    auto_increment_field: str | None = None
    authorize: FilterExpr | None = None
    # Field-level default values (FM-32): literal values stamped onto a NEW
    # document (insert/replace/upsert-insert) that omits the key. Wire name is
    # `defaults`, omitted when empty (server `BTreeMap::is_empty` skip rule).
    defaults: dict[str, Any] = Field(default_factory=dict)
    # FM-33: `delete` steps stamp a tombstone instead of removing the row (see
    # the harness / server txn docs). Wire key `softDelete`, omitted when false
    # (server `skip_serializing_if = "is_false"`).
    soft_delete: bool = False
    # ENH-028: computed fields — a `ValueExpr` per declared field, re-evaluated
    # by the server on every write and stored (declarative denormalization).
    # Client-supplied values are dropped before validation; a null result
    # removes the key. Wire name is `computed`, omitted when empty (server
    # `BTreeMap::is_empty` skip rule).
    computed: dict[str, ValueExpr] = Field(default_factory=dict)

    @model_serializer(mode="wrap")
    def _drop_absent_owner(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("ownerField") is None:
            out.pop("ownerField", None)
        if out.get("collaboratorsField") is None:
            out.pop("collaboratorsField", None)
        # `ttl` is a behavior toggle (server skip_serializing_if = "Option::is_none").
        if out.get("ttl") is None:
            out.pop("ttl", None)
        # `updatedAtField` is an opt-in stamp policy (FM-36, server
        # skip_serializing_if = "Option::is_none"); omit it on the wire when unset.
        if out.get("updatedAtField") is None:
            out.pop("updatedAtField", None)
        # `autoIncrementField` is likewise an opt-in table option (FM-37,
        # server skip_serializing_if = "Option::is_none").
        if out.get("autoIncrementField") is None:
            out.pop("autoIncrementField", None)
        # `authorize` is an opt-in predicate (server skip_serializing_if =
        # "Option::is_none"); omit it on the wire when unset.
        if out.get("authorize") is None:
            out.pop("authorize", None)
        # `defaults` is omitted on the wire when empty (server
        # skip_serializing_if = "BTreeMap::is_empty").
        if not out.get("defaults"):
            out.pop("defaults", None)
        # `softDelete` is a bool flag (server skip_serializing_if = "is_false"):
        # drop False, keep True.
        if not out.get("softDelete"):
            out.pop("softDelete", None)
        # `computed` is omitted on the wire when empty (server
        # skip_serializing_if = "BTreeMap::is_empty").
        if not out.get("computed"):
            out.pop("computed", None)
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
        self._collaborators: str | None = None
        self._authorize: FilterExpr | None = None
        self._defaults: dict[str, Any] | None = None
        self._soft_delete: bool = False
        self._updated_at: str | None = None
        self._auto_increment: str | None = None
        self._computed: dict[str, Any] | None = None

    def field(self, name: str, ft: Any) -> TableBuilder:
        """Declare a typed field ``name`` of type ``ft`` (a ``t.*`` constructor dict)."""
        self._fields[name] = ft
        return self

    def index(self, name: str, fields: list[str]) -> TableBuilder:
        """Declare a btree index over ``fields``. Chain ``.unique()`` /
        ``.where(pred)`` to refine it (see those methods)."""
        self._indexes.append({"name": name, "fields": fields})
        return self

    def unique(self) -> TableBuilder:
        """Mark the most recently declared index as unique
        (``.index(...).unique()``), mirroring the TS/Rust chainable. The server
        emits ``CREATE UNIQUE INDEX`` over the index's declared ``fields`` (never
        ``id``/``created_at``). No-op if no index has been declared yet."""
        if self._indexes:
            self._indexes[-1]["unique"] = True
        return self

    def where(self, predicate: FilterExpr) -> TableBuilder:
        """Attach a partial-index predicate to the most recently declared index
        (``.index(...).where(pred)``), mirroring the TS client. Serialized as the
        wire key ``where`` — byte-identical to the server/Rust/TS clients. The
        predicate reuses the query-time ``FilterExpr`` type (compiled to literal
        SQL at DDL time on the server). No-op if no index has been declared yet."""
        if self._indexes:
            self._indexes[-1]["where"] = predicate
        return self

    def search_index(
        self,
        name: str,
        fields: list[str],
        *,
        language: str | None = None,
    ) -> TableBuilder:
        """Declare a full-text-search index over ``fields``.

        ``language`` optionally selects a Postgres ``regconfig`` name (e.g.
        ``"english"``, ``"simple"``, ``"spanish"``) for stemming and stop-words;
        it defaults to ``"english"`` server-side and is only added to the index
        when a value is provided so existing schemas serialize byte-identically.
        See ENH-006."""
        entry: dict[str, Any] = {"name": name, "fields": fields, "search": True}
        if language is not None:
            entry["language"] = language
        self._indexes.append(entry)
        return self

    def vector_index(
        self,
        name: str,
        field: str,
        dimensions: int,
        *,
        filter_fields: list[str] | None = None,
        metric: Literal["cosine", "l2", "ip"] | None = None,
    ) -> TableBuilder:
        """Declare a vector index over ``field`` (a ``t.vector(dimensions)`` column).

        ``filter_fields`` adds prefilter columns (``filterFields`` on the wire),
        omitted when empty. ``metric`` selects the distance metric; it defaults to
        ``"cosine"`` server-side and is only added to the spec when set to a
        non-default value (``"l2"`` or ``"ip"``) so existing schemas serialize
        byte-identically."""
        spec: dict[str, Any] = {"dimensions": dimensions}
        if filter_fields:
            spec["filterFields"] = list(filter_fields)
        if metric is not None and metric != "cosine":
            spec["metric"] = metric
        self._indexes.append({"name": name, "fields": [field], "vector": spec})
        return self

    def owner_field(self, name: str) -> TableBuilder:
        """Declare the per-row owner field. ``name`` must reference a declared
        id (or array-of-id) field whose value is the owning user id; the server
        stamps it on insert and enforces owner-only read/mutate. Round-tripped
        on the wire as ``ownerField``."""
        self._owner = name
        return self

    def collaborators_field(self, name: str) -> TableBuilder:
        """Declare the per-row collaborators field. ``name`` must reference a
        declared array-of-strings (or array-of-id) field whose values are
        additional user ids that may read/mutate the row (owner OR
        collaborator). Server-enforced; round-tripped on the wire as
        ``collaboratorsField``."""
        self._collaborators = name
        return self

    def authorize(self, predicate: FilterExpr) -> TableBuilder:
        """Declare the per-row authorization predicate (Model C). ``predicate``
        is a ``FilterExpr`` over this table's declared doc fields and the
        principal's markers (``{"$user": true}`` / ``{"$email": true}``).
        Enforced on the same read/write/subscription seams as ``ownerField``;
        additive to it. Marker values are valid only here — client ``.filter()``
        queries reject them. Server-enforced; round-tripped on the wire as
        ``authorize``."""
        self._authorize = predicate
        return self

    def defaults(self, values: dict[str, Any]) -> TableBuilder:
        """Declare field-level default values (FM-32). Each key must be a
        declared field and each value a non-null literal matching that field's
        type (server-validated at schema-push time). Applied to a NEW document
        (insert/replace/upsert-insert) that omits the key; ``patch`` never
        re-applies, so clearing an optional field stays cleared. Round-tripped
        on the wire as ``defaults``, omitted when empty."""
        self._defaults = dict(values)
        return self

    def soft_delete(self) -> TableBuilder:
        """Declare this table soft-delete (FM-33): ``delete``/``deleteByQuery``
        stamp a ``deleted_at`` tombstone — invisible to every read and write
        lookup — instead of removing the row, and an ``undelete(table, id)``
        txn step restores it. The TTL reaper still hard-deletes. Round-tripped
        on the wire as ``softDelete``, omitted when unset."""
        self._soft_delete = True
        return self

    def updated_at_field(self, name: str) -> TableBuilder:
        """Declare the server-stamped updated-at field (FM-36). ``name`` must
        reference a declared ``number`` or ``int64`` field; the server stamps it
        with the current epoch-ms on every version-bumping write — insert,
        patch, replace, upsert (both branches), patchByQuery, and cascade
        setNull — overwriting any client-supplied value. The value follows the
        field's wire convention: a JSON number on a ``number`` field, a decimal
        string on an ``int64`` field. The client only declares the field and
        round-trips it on the wire as ``updatedAtField``."""
        self._updated_at = name
        return self

    def auto_increment_field(self, name: str) -> TableBuilder:
        """Declare the server-assigned auto-increment counter field (FM-37).
        ``name`` must reference a declared ``int64`` field; the server stamps it
        with the next value of a per-table monotonic counter on insert and on
        upsert's insert branch — overwriting any client-supplied value (a
        ``defaults`` entry on the field loses the same way). After insert the
        field is immutable: a patch or replace that changes the stored value
        is rejected, while round-tripping it unchanged is allowed. The value is
        a decimal string (the int64 wire convention) and is legal in a unique
        index (the ticket-number guarantee). The client only declares the field
        and round-trips it on the wire as ``autoIncrementField``."""
        self._auto_increment = name
        return self

    def computed(self, name: str, expr: Any) -> TableBuilder:
        """Declare a computed field (ENH-028): ``name`` must be a declared
        field (not the table's ``ownerField``/``collaboratorsField``/
        ``autoIncrementField``), re-derived by the server on every write —
        insert, patch, replace, upsert, patchByQuery, and cascade ``setNull``
        — from ``expr`` (a :class:`par_rt_db.value_expr.ValueExpr`, built with
        the ``ve.*`` constructors) over the final doc. Client-supplied values
        are dropped before validation; a null result removes the key; an
        evaluation error fails the write with ``BAD_REQUEST`` naming the field.
        The declared field may be indexed (that is the point — derived values
        become queryable). Round-tripped on the wire as ``computed``, omitted
        when empty."""
        if self._computed is None:
            self._computed = {}
        self._computed[name] = expr
        return self

    def _build(self) -> dict[str, Any]:
        out: dict[str, Any] = {"fields": self._fields, "indexes": self._indexes}
        if self._owner is not None:
            out["ownerField"] = self._owner
        if self._collaborators is not None:
            out["collaboratorsField"] = self._collaborators
        if self._authorize is not None:
            out["authorize"] = self._authorize
        if self._defaults:
            out["defaults"] = self._defaults
        if self._soft_delete:
            out["softDelete"] = True
        if self._updated_at is not None:
            out["updatedAtField"] = self._updated_at
        if self._auto_increment is not None:
            out["autoIncrementField"] = self._auto_increment
        if self._computed:
            out["computed"] = self._computed
        return out


class SchemaBuilder:
    """Fluent builder for a whole schema."""

    def __init__(self) -> None:
        self._tables: dict[str, dict[str, Any]] = {}

    def table(self, name: str, configure: Any) -> SchemaBuilder:
        """Declare a table named ``name``. ``configure`` is a callable that
        receives a :class:`TableBuilder` to populate the table's fields,
        indexes, and per-row options."""
        tb = TableBuilder()
        configure(tb)
        self._tables[name] = tb._build()
        return self

    def build(self) -> SchemaDef:
        """Validate the accumulated tables into a :class:`SchemaDef`."""
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
    def id(table: str, on_delete: OnDeleteAction | None = None) -> dict[str, Any]:
        """``Id<Table>`` field referencing documents in ``table`` (a typed ``str``).

        ``on_delete`` (FM-33) declares the app-level action taken when the
        referenced row is hard-deleted: ``"cascade"`` (delete the children too,
        recursively), ``"restrict"`` (reject the parent delete with a conflict
        while live children exist), or ``"setNull"`` (clear the field on each
        child — requires wrapping in ``t.optional(t.id(...))``). Server-validated
        at push time (the field needs a single-field, non-unique, non-partial
        btree index; the referenced table must be declared). Omitted from the
        wire when ``None``."""
        out: dict[str, Any] = {"type": "id", "table": table}
        if on_delete is not None:
            out["onDelete"] = on_delete
        return out

    @staticmethod
    def literal(value: Any) -> dict[str, Any]:
        """Literal singleton field holding the constant ``value``."""
        return {"type": "literal", "value": value}

    @staticmethod
    def optional(inner: Any) -> dict[str, Any]:
        """Optional field wrapping ``inner`` (a field type); absent values are omitted."""
        return {"type": "optional", "inner": inner}

    @staticmethod
    def union(variants: list[Any]) -> dict[str, Any]:
        """Union of alternative ``variants`` (a list of field types)."""
        return {"type": "union", "variants": variants}

    @staticmethod
    def array(element: Any) -> dict[str, Any]:
        """Array field whose elements are all of type ``element``."""
        return {"type": "array", "element": element}

    @staticmethod
    def object(fields: dict[str, Any]) -> dict[str, Any]:
        """Nested object field mapping each name to a field type."""
        return {"type": "object", "fields": fields}

    @staticmethod
    def record(value: Any) -> dict[str, Any]:
        """Record field: a string-keyed map whose values are all of type ``value``."""
        return {"type": "record", "value": value}

    @staticmethod
    def vector(dimensions: int) -> dict[str, Any]:
        """Embedding vector field of fixed ``dimensions`` (the column a vector index scans)."""
        return {"type": "vector", "dimensions": dimensions}


t = _SchemaNamespace()

Schema = SimpleNamespace(
    builder=SchemaBuilder,
    model_validate=SchemaDef.model_validate,
)
