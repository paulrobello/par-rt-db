"""Query lookup expansion mirror tests (python-client view).

Ports the behavioral slice of ``server/tests/lookup_test.rs`` and the wire
mirror rules of ``ts-client/src/query.ts``'s ``.lookup()`` to the python
client: the ``LookupSpec`` wire shape (``as`` alias, cap omit-when-None,
skip-when-empty list), the ``TableQuery.lookup`` builder, compile-time
validation (error codes), and in-memory expansion semantics (single-Id →
doc-or-null, Array<Id> → declared order with dangling ids dropped and ``cap``
truncation). The full behavioral corpus lives in ``wire-corpus/semantics/``
(``lookup-*``, ``query-combo-lookups-excludes-*``), executed by
``tests/test_semantics_corpus.py``; this file pins the python-only surfaces
the corpus cannot (the builder and the pydantic wire model).
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from par_rt_db import Mutation
from par_rt_db.errors import ErrorCode, RtDbError
from par_rt_db.in_memory import InMemoryRtDbClient
from par_rt_db.query import Query, TableQuery, _dump_query
from par_rt_db.schema import Schema, t


def _schema() -> Any:
    """users + posts; posts carries an optional single-Id and an Array<Id> field."""
    return (
        Schema.builder()
        .table(
            "users",
            lambda tb: tb.field("name", t.string()).index("by_name", ["name"]),
        )
        .table(
            "posts",
            lambda tb: (
                tb.field("title", t.string())
                .field("authorId", t.optional(t.id("users")))
                .field("tagIds", t.array(t.id("users")))
                # an id-reference field may carry a btree index; the Array<Id>
                # field stays unindexed (matching the corpus fixtures)
                .index("by_author", ["authorId"])
            ),
        )
        .build()
    )


def _new_client() -> InMemoryRtDbClient:
    c = InMemoryRtDbClient()
    c.push_schema(_schema())
    return c


def _id_of(res: Any) -> str:
    """Extract the ``id`` of an insert ``StepResult`` (pyright: the union also
    carries shapes with no ``id``, so attribute access does not type-check)."""
    assert res is not None
    return str(res.model_dump()["id"])


def _seed() -> tuple[InMemoryRtDbClient, str, str]:
    """Insert two users and three posts; return (client, ann_id, p1_id)."""
    c = _new_client()
    r = c.mutate(
        Mutation.builder().insert("users", {"name": "Ann"}).insert("users", {"name": "Bo"}).build()
    )
    ann = _id_of(r[0])
    bo = _id_of(r[1])
    r2 = c.mutate(
        Mutation.builder()
        .insert("posts", {"title": "p1", "authorId": ann, "tagIds": [ann, bo]})
        .insert("posts", {"title": "p2", "tagIds": [bo]})
        .insert("posts", {"title": "p3", "authorId": "f" * 32, "tagIds": ["e" * 32]})
        .build()
    )
    return c, ann, _id_of(r2[0])


def _run(c: InMemoryRtDbClient, q: TableQuery) -> Any:
    return c.run_query(q.build())


# --- wire / builder -----------------------------------------------------------


def test_lookup_builder_wire_shape() -> None:
    """`.lookup()` appends a LookupSpec; the wire key is `as`, cap omitted
    when None, and the list omitted entirely when no lookups were chained."""
    wire = _dump_query(TableQuery("posts").lookup("authorId", "users", "author"))
    assert wire == {
        "table": "posts",
        "lookups": [{"field": "authorId", "table": "users", "as": "author"}],
    }
    wire2 = _dump_query(
        TableQuery("posts")
        .lookup("authorId", "users", "author", cap=20)
        .lookup("tagIds", "users", "taggers")
    )
    assert wire2["lookups"] == [
        {"field": "authorId", "table": "users", "as": "author", "cap": 20},
        {"field": "tagIds", "table": "users", "as": "taggers"},
    ]
    assert "lookups" not in _dump_query(TableQuery("posts"))


def test_lookup_query_model_parses_as_alias() -> None:
    """The wire `as` key parses into the model and re-serializes identically."""
    q = Query.model_validate(
        {
            "table": "posts",
            "lookups": [{"field": "authorId", "table": "users", "as": "author", "cap": 5}],
        }
    )
    assert q.lookups is not None and q.lookups[0].as_ == "author"
    assert q.lookups[0].cap == 5
    assert q.model_dump(by_alias=True, mode="json")["lookups"][0] == {
        "field": "authorId",
        "table": "users",
        "as": "author",
        "cap": 5,
    }


def test_lookup_query_model_rejects_unknown_fields() -> None:
    """``extra="forbid"`` mirrors the server's ``deny_unknown_fields``."""
    with pytest.raises(ValidationError):
        Query.model_validate(
            {
                "table": "posts",
                "lookups": [{"field": "f", "table": "t", "as": "a", "bogus": 1}],
            }
        )


# --- validation error codes -----------------------------------------------------


def test_lookup_unknown_table_not_found() -> None:
    c, _, _ = _seed()
    with pytest.raises(RtDbError) as ei:
        _run(c, TableQuery("posts").lookup("authorId", "ghosts", "author"))
    assert ei.value.code == ErrorCode.NOT_FOUND


def test_lookup_field_not_declared_bad_request() -> None:
    c, _, _ = _seed()
    with pytest.raises(RtDbError) as ei:
        _run(c, TableQuery("posts").lookup("nope", "users", "author"))
    assert ei.value.code == ErrorCode.BAD_REQUEST


def test_lookup_field_not_id_family_bad_request() -> None:
    c, _, _ = _seed()
    with pytest.raises(RtDbError) as ei:
        _run(c, TableQuery("posts").lookup("title", "users", "author"))
    assert ei.value.code == ErrorCode.BAD_REQUEST


def test_lookup_table_mismatch_bad_request() -> None:
    c, _, _ = _seed()
    with pytest.raises(RtDbError) as ei:
        _run(c, TableQuery("posts").lookup("authorId", "posts", "author"))
    assert ei.value.code == ErrorCode.BAD_REQUEST


def test_lookup_alias_collisions_bad_request() -> None:
    c, _, _ = _seed()
    # single-lookup collisions: `_`-prefixed alias, declared-field collision
    for field, table, alias in (
        ("authorId", "users", "_hidden"),  # `_`-prefixed alias
        ("authorId", "users", "title"),  # collides with a declared field
    ):
        with pytest.raises(RtDbError) as ei:
            _run(c, TableQuery("posts").lookup(field, table, alias))
        assert ei.value.code == ErrorCode.BAD_REQUEST
    # duplicate aliases: only the pair (two lookups, same `as`) is an error
    with pytest.raises(RtDbError) as ei:
        _run(
            c,
            TableQuery("posts").lookup("authorId", "users", "dup").lookup("tagIds", "users", "dup"),
        )
    assert ei.value.code == ErrorCode.BAD_REQUEST


def test_lookup_cap_overflow_bad_request() -> None:
    c, _, _ = _seed()
    with pytest.raises(RtDbError) as ei:
        _run(c, TableQuery("posts").lookup("tagIds", "users", "tags", cap=1001))
    assert ei.value.code == ErrorCode.BAD_REQUEST


def test_lookup_too_many_bad_request() -> None:
    c, _, _ = _seed()
    q = TableQuery("posts")
    for i in range(5):
        # five DISTINCT aliases so only the count rule can fire
        q = q.lookup("authorId", "users", f"alias_{i}")
    with pytest.raises(RtDbError) as ei:
        _run(c, q)
    assert ei.value.code == ErrorCode.BAD_REQUEST


def test_lookup_excludes_docless_terminals() -> None:
    """lookups + count/distinct/aggregate is BAD_REQUEST (the combination table)."""
    c, _, _ = _seed()
    builders = (
        lambda q: q.count(),
        lambda q: q.distinct(),
        lambda q: q.aggregate("count"),
    )
    for terminal in builders:
        q = TableQuery("posts").with_index("by_author").eq("x").lookup("authorId", "users", "a")
        terminal(q)
        with pytest.raises(RtDbError) as ei:
            _run(c, q)
        assert ei.value.code == ErrorCode.BAD_REQUEST


# --- expansion semantics --------------------------------------------------------


def test_lookup_single_join_with_system_fields() -> None:
    """collect + single-Id lookup: joined doc present with child system fields."""
    c, ann, _ = _seed()
    out = _run(
        c,
        TableQuery("posts").with_index("by_author").eq(ann).lookup("authorId", "users", "author"),
    )
    assert len(out) == 1
    doc = out[0]
    assert isinstance(doc["author"], dict)
    assert doc["author"]["name"] == "Ann"
    assert doc["author"]["_id"] == ann
    assert "_creationTime" in doc["author"] and "_version" in doc["author"]


def test_lookup_single_missing_optional_reads_null() -> None:
    """A null/absent optional Id and a dangling id both read as null."""
    c, _, _ = _seed()
    out = _run(c, TableQuery("posts").lookup("authorId", "users", "author"))
    by_title = {d["title"]: d["author"] for d in out}
    assert by_title["p1"]["name"] == "Ann"
    assert by_title["p2"] is None  # absent optional
    assert by_title["p3"] is None  # dangling id


def test_lookup_array_order_cap_dangling() -> None:
    """Array<Id> lookup: declared id-array order, dangling dropped, cap truncates."""
    c, ann, _ = _seed()
    out = _run(c, TableQuery("posts").lookup("tagIds", "users", "tags"))
    by_title = {d["title"]: d["tags"] for d in out}
    assert [u["name"] for u in by_title["p1"]] == ["Ann", "Bo"]
    assert [u["name"] for u in by_title["p2"]] == ["Bo"]  # order preserved
    assert by_title["p3"] == []  # dangling-only array reads empty
    capped = _run(c, TableQuery("posts").lookup("tagIds", "users", "tags", cap=1))
    by_title = {d["title"]: d["tags"] for d in capped}
    assert [u["name"] for u in by_title["p1"]] == ["Ann"]  # first-in-array kept


def test_lookup_projection_names_alias() -> None:
    """`fields` may name the alias; an unknown name is still BAD_REQUEST."""
    c, _, _ = _seed()
    out = _run(
        c,
        TableQuery("posts").lookup("authorId", "users", "author").fields("title", "author"),
    )
    p1 = next(d for d in out if d["title"] == "p1")
    assert set(p1) == {"title", "author", "_id", "_creationTime", "_version"}
    assert p1["author"]["name"] == "Ann"
    with pytest.raises(RtDbError) as ei:
        _run(c, TableQuery("posts").lookup("authorId", "users", "author").fields("ghost"))
    assert ei.value.code == ErrorCode.BAD_REQUEST


def test_lookup_get_terminal() -> None:
    """get + lookup: the single doc gains the joined key."""
    c, ann, p1 = _seed()
    out = _run(c, TableQuery("posts").get(p1).lookup("authorId", "users", "author"))
    assert out is not None
    assert out["author"]["_id"] == ann
    assert out["author"]["name"] == "Ann"
