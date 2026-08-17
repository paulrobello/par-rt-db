"""Tests for FM-33 (cascade delete + soft delete) in the in-memory harness.

Mirrors ``server/tests/cascade_test.rs``: ``onDelete`` actions
(``cascade``/``restrict``/``setNull``) expand on a hard delete (recursive,
cycle-guarded, row-budgeted); a ``softDelete`` table stamps a ``deleted_at``
tombstone that every read and write lookup filters; ``undelete`` restores.
Push-time validation rejects the same shapes the server rejects, with the
server's exact ``SCHEMA_VIOLATION`` messages.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import TypeAdapter

from par_rt_db import Mutation, TableQuery
from par_rt_db.errors import ErrorCode, RtDbError
from par_rt_db.in_memory import InMemoryRtDbClient, InMemoryRtDbClientOptions
from par_rt_db.schema import Schema, SchemaDef
from par_rt_db.wire import FilterExpr


def _flt(expr: dict[str, object]) -> FilterExpr:
    return TypeAdapter(FilterExpr).validate_python(expr)


def _schema(tables: dict[str, Any]) -> SchemaDef:
    # ``Schema`` is a runtime namespace (builder/model_validate), so type
    # annotations use the underlying pydantic model.
    return Schema.model_validate({"tables": tables})


def _new_client(schema: SchemaDef) -> InMemoryRtDbClient:
    clock = [10_000]

    def now() -> int:
        return clock[0]

    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=now, random=lambda: 0.5))
    c.push_schema(schema)
    return c


def _insert(c: InMemoryRtDbClient, table: str, doc: dict[str, Any]) -> str:
    [res] = c.mutate(Mutation.builder().insert(table, doc).build())
    assert res is not None
    return str(res.model_dump()["id"])


def _count(c: InMemoryRtDbClient, table: str) -> int:
    result: int = c.run_query(TableQuery(table).count().build())
    return result


# --- fixtures ---------------------------------------------------------------

_USERS: dict[str, Any] = {"fields": {"name": {"type": "string"}}, "indexes": []}


def _cascade_schema(*, comments_soft: bool = True) -> SchemaDef:
    """users <- posts (cascade) <- comments (cascade, softDelete); audit
    (restrict) and drafts (setNull) also reference users. Table order matters
    for the rollback test: audit's restrict fires BEFORE drafts' setNull."""
    return _schema(
        {
            "users": _USERS,
            "posts": {
                "fields": {
                    "title": {"type": "string"},
                    "authorId": {"type": "id", "table": "users", "onDelete": "cascade"},
                },
                "indexes": [{"name": "by_author", "fields": ["authorId"]}],
            },
            "comments": {
                "fields": {
                    "body": {"type": "string"},
                    "postId": {"type": "id", "table": "posts", "onDelete": "cascade"},
                },
                "indexes": [{"name": "by_post", "fields": ["postId"]}],
                "softDelete": comments_soft,
            },
            "audit": {
                "fields": {
                    "tag": {"type": "string"},
                    "userId": {"type": "id", "table": "users", "onDelete": "restrict"},
                },
                "indexes": [{"name": "by_user", "fields": ["userId"]}],
            },
            "drafts": {
                "fields": {
                    "note": {"type": "string"},
                    "userId": {
                        "type": "optional",
                        "inner": {"type": "id", "table": "users", "onDelete": "setNull"},
                    },
                },
                "indexes": [{"name": "by_user", "fields": ["userId"]}],
            },
        }
    )


def _seed_tree(c: InMemoryRtDbClient) -> dict[str, str]:
    """One user -> one post -> one comment, plus one audit row and one draft."""
    uid = _insert(c, "users", {"name": "u"})
    pid = _insert(c, "posts", {"title": "p", "authorId": uid})
    cid = _insert(c, "comments", {"body": "c", "postId": pid})
    aid = _insert(c, "audit", {"tag": "a", "userId": uid})
    did = _insert(c, "drafts", {"note": "d", "userId": uid})
    return {"uid": uid, "pid": pid, "cid": cid, "aid": aid, "did": did}


# --- cascade ----------------------------------------------------------------


def test_cascade_hard_deletes_children_and_soft_stamps_soft_tables() -> None:
    c = _new_client(_cascade_schema())
    ids = _seed_tree(c)
    c.mutate(Mutation.builder().delete("audit", ids["aid"]).build())

    c.mutate(Mutation.builder().delete("users", ids["uid"]).build())

    # users/posts hard-deleted; comments soft-stamped (present but filtered).
    assert c.get("users", ids["uid"]) is None
    assert c.get("posts", ids["pid"]) is None
    assert c.get("comments", ids["cid"]) is None
    assert _count(c, "comments") == 0
    row = c._docs[("comments", ids["cid"])]
    assert row.deleted_at == 10_000  # the frozen clock
    assert row.version == 2  # the stamp bumps version (server parity)


def test_cascade_set_null_removes_child_field_key_and_bumps_version() -> None:
    c = _new_client(_cascade_schema())
    ids = _seed_tree(c)
    before = c.get("drafts", ids["did"])
    assert before is not None and before["_version"] == 1

    c.mutate(Mutation.builder().delete("audit", ids["aid"]).build())
    c.mutate(Mutation.builder().delete("users", ids["uid"]).build())

    draft = c.get("drafts", ids["did"])
    assert draft is not None
    assert "userId" not in draft  # setNull REMOVES the key (unset semantics)
    assert draft["_version"] == 2


def test_restrict_conflicts_while_live_children_exist_then_succeeds() -> None:
    c = _new_client(_cascade_schema())
    ids = _seed_tree(c)

    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().delete("users", ids["uid"]).build())
    err = ei.value
    assert err.code == ErrorCode.CONFLICT
    assert err.message == (
        f"cannot delete 'users': 'audit.userId' is referenced by document '{ids['aid']}'"
    )
    # Nothing was applied — the failed txn rolled back atomically.
    assert c.get("users", ids["uid"]) is not None
    assert c.get("posts", ids["pid"]) is not None
    assert _count(c, "comments") == 1
    draft = c.get("drafts", ids["did"])
    assert draft is not None and "userId" in draft

    # With the restrict row gone the same delete cascades through.
    c.mutate(Mutation.builder().delete("audit", ids["aid"]).build())
    c.mutate(Mutation.builder().delete("users", ids["uid"]).build())
    assert c.get("users", ids["uid"]) is None


def test_restrict_and_cascade_ignore_soft_deleted_children() -> None:
    """A soft-deleted child is invisible to every onDelete action."""
    schema = _schema(
        {
            "parents": _USERS,
            "children": {
                "fields": {
                    "label": {"type": "string"},
                    "parentId": {"type": "id", "table": "parents", "onDelete": "restrict"},
                },
                "indexes": [{"name": "by_parent", "fields": ["parentId"]}],
                "softDelete": True,
            },
        }
    )
    c = _new_client(schema)
    pid = _insert(c, "parents", {"name": "p"})
    cid = _insert(c, "children", {"label": "c", "parentId": pid})
    c.mutate(Mutation.builder().delete("children", cid).build())  # soft-stamp

    # The soft-deleted child no longer restricts the parent delete.
    c.mutate(Mutation.builder().delete("parents", pid).build())
    assert c.get("parents", pid) is None


def test_delete_by_query_cascades_with_shared_visited_and_budget() -> None:
    c = _new_client(_cascade_schema())
    ids = _seed_tree(c)
    c.mutate(Mutation.builder().delete("audit", ids["aid"]).build())
    uid2 = _insert(c, "users", {"name": "u2"})
    pid2 = _insert(c, "posts", {"title": "p2", "authorId": uid2})

    [res] = c.mutate(
        Mutation.builder()
        .delete_by_query("users", _flt({"op": "neq", "field": "name", "value": "zzz"}))
        .build()
    )
    assert res is not None
    dumped = res.model_dump(by_alias=True)
    assert dumped["deleted"] == 2 and dumped["truncated"] is False

    # Both users and their whole subtrees went.
    assert _count(c, "users") == 0
    assert c.get("posts", ids["pid"]) is None
    assert c.get("posts", pid2) is None
    stamped = c._docs.get(("comments", ids["cid"]))
    assert stamped is not None
    assert stamped.deleted_at is not None


def test_self_reference_cycle_guard_terminates() -> None:
    schema = _schema(
        {
            "nodes": {
                "fields": {
                    "label": {"type": "string"},
                    "parentId": {
                        "type": "optional",
                        "inner": {"type": "id", "table": "nodes", "onDelete": "cascade"},
                    },
                },
                "indexes": [{"name": "by_parent", "fields": ["parentId"]}],
            },
        }
    )
    c = _new_client(schema)
    a = _insert(c, "nodes", {"label": "a"})
    b = _insert(c, "nodes", {"label": "b", "parentId": a})
    c.mutate(Mutation.builder().patch("nodes", a, {"parentId": b}).build())  # cycle a <-> b

    c.mutate(Mutation.builder().delete("nodes", a).build())
    assert _count(c, "nodes") == 0  # visited set stopped the mutual recursion


def test_cascade_budget_conflict(monkeypatch: pytest.MonkeyPatch) -> None:
    """MAX_CASCADE_ROWS bounds rows per initiating delete step (shared across a
    deleteByQuery); over-budget raises CONFLICT and rolls the whole txn back."""
    monkeypatch.setattr("par_rt_db.in_memory.store.MAX_CASCADE_ROWS", 3)
    schema = _schema(
        {
            "parents": _USERS,
            "children": {
                "fields": {
                    "label": {"type": "string"},
                    "parentId": {"type": "id", "table": "parents", "onDelete": "cascade"},
                },
                "indexes": [{"name": "by_parent", "fields": ["parentId"]}],
            },
        }
    )
    c = _new_client(schema)
    pid = _insert(c, "parents", {"name": "p"})
    for i in range(4):  # initiator + 4 children = 5 > budget of 3
        _insert(c, "children", {"label": f"c{i}", "parentId": pid})

    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().delete("parents", pid).build())
    assert ei.value.code == ErrorCode.CONFLICT
    assert ei.value.message == "onDelete cascade exceeds the limit of 3 rows"
    # Rolled back atomically.
    assert c.get("parents", pid) is not None
    assert _count(c, "children") == 4


# --- soft delete -------------------------------------------------------------


def _soft_schema() -> SchemaDef:
    return _schema(
        {
            "items": {
                "fields": {"label": {"type": "string"}},
                "indexes": [{"name": "by_label", "fields": ["label"], "unique": True}],
                "softDelete": True,
            },
        }
    )


def test_soft_delete_filters_every_read_terminal() -> None:
    c = _new_client(_soft_schema())
    a = _insert(c, "items", {"label": "x"})
    c.mutate(Mutation.builder().delete("items", a).build())

    assert c.get("items", a) is None
    assert c.run_query(TableQuery("items").collect().build()) == []
    assert _count(c, "items") == 0


def test_soft_delete_then_reinsert_same_unique_key() -> None:
    """A soft-deleted row is outside the unique predicate — the key frees up."""
    c = _new_client(_soft_schema())
    a = _insert(c, "items", {"label": "x"})
    c.mutate(Mutation.builder().delete("items", a).build())

    b = _insert(c, "items", {"label": "x"})  # would CONFLICT if a were live
    assert b != a
    assert _count(c, "items") == 1


def test_upsert_over_soft_deleted_key_inserts_fresh() -> None:
    c = _new_client(_soft_schema())
    a = _insert(c, "items", {"label": "x"})
    c.mutate(Mutation.builder().delete("items", a).build())

    [res] = c.mutate(
        Mutation.builder()
        .upsert("items", "by_label", ["x"], {"label": "x"}, {"label": "y"})
        .build()
    )
    assert res is not None
    dumped = res.model_dump(by_alias=True)
    assert dumped["inserted"] is True  # matched nothing — the soft row is absent
    assert dumped["id"] != a


def test_soft_deleted_row_absent_to_write_lookups() -> None:
    c = _new_client(_soft_schema())
    a = _insert(c, "items", {"label": "x"})
    c.mutate(Mutation.builder().delete("items", a).build())

    for build in (
        lambda: Mutation.builder().patch("items", a, {"label": "z"}),
        lambda: Mutation.builder().replace("items", a, {"label": "z"}),
        lambda: Mutation.builder().expect_version("items", a, 2),
    ):
        with pytest.raises(RtDbError) as ei:
            c.mutate(build().build())
        assert ei.value.code == ErrorCode.NOT_FOUND
    # Deleting an already-soft-deleted row is also NOT_FOUND.
    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().delete("items", a).build())
    assert ei.value.code == ErrorCode.NOT_FOUND


def test_soft_delete_never_triggers_cascade() -> None:
    """Deleting a softDelete parent stamps it — children are untouched."""
    schema = _schema(
        {
            "parents": {
                "fields": {"name": {"type": "string"}},
                "indexes": [],
                "softDelete": True,
            },
            "children": {
                "fields": {
                    "label": {"type": "string"},
                    "parentId": {"type": "id", "table": "parents", "onDelete": "cascade"},
                },
                "indexes": [{"name": "by_parent", "fields": ["parentId"]}],
            },
        }
    )
    c = _new_client(schema)
    pid = _insert(c, "parents", {"name": "p"})
    cid = _insert(c, "children", {"label": "c", "parentId": pid})

    c.mutate(Mutation.builder().delete("parents", pid).build())
    assert c.get("children", cid) is not None  # cascade did not fire
    assert c._docs[("parents", pid)].deleted_at is not None


# --- undelete ----------------------------------------------------------------


def test_undelete_restores_and_is_idempotent() -> None:
    c = _new_client(_soft_schema())
    a = _insert(c, "items", {"label": "x"})
    c.mutate(Mutation.builder().delete("items", a).build())
    stamped_version = c._docs[("items", a)].version

    c.mutate(Mutation.builder().undelete("items", a).build())
    doc = c.get("items", a)
    assert doc is not None and doc["label"] == "x"
    assert c._docs[("items", a)].deleted_at is None
    assert c._docs[("items", a)].version == stamped_version + 1

    # Undeleting a live row is an idempotent no-op (server parity).
    c.mutate(Mutation.builder().undelete("items", a).build())
    assert c._docs[("items", a)].version == stamped_version + 1


def test_undelete_not_found_and_non_soft_table() -> None:
    c = _new_client(_cascade_schema())
    # The softDelete gate runs BEFORE the row lookup (server order), so even a
    # missing id on a plain table is BAD_REQUEST.
    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().undelete("users", "deadbeef").build())
    assert ei.value.code == ErrorCode.BAD_REQUEST

    # A soft-delete table with a genuinely missing row is NOT_FOUND.
    cid = _insert(c, "comments", {"body": "c", "postId": "0" * 32})
    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().undelete("comments", "deadbeef").build())
    assert ei.value.code == ErrorCode.NOT_FOUND
    # ...and a table without softDelete rejects undelete outright.
    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().undelete("users", cid).build())
    assert ei.value.code == ErrorCode.BAD_REQUEST
    assert ei.value.message == "table 'users' does not declare softDelete"


def test_undelete_conflicts_with_unique_index_when_key_taken() -> None:
    """Restoring a row whose unique key was re-inserted hits the unique check."""
    c = _new_client(_soft_schema())
    a = _insert(c, "items", {"label": "x"})
    c.mutate(Mutation.builder().delete("items", a).build())
    b = _insert(c, "items", {"label": "x"})

    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().undelete("items", a).build())
    assert ei.value.code == ErrorCode.CONFLICT
    assert c.get("items", b) is not None
    assert c.get("items", a) is None  # the failed undelete rolled back


# --- TTL reaper: always force-hard, cascades when referenced ------------------


def test_reaper_force_hard_deletes_soft_deleted_rows() -> None:
    schema = _schema(
        {
            "sessions": {
                "fields": {"expiresAt": {"type": "number"}},
                "indexes": [{"name": "by_expires", "fields": ["expiresAt"]}],
                "ttl": {"field": "expiresAt"},
                "softDelete": True,
            },
        }
    )
    c = _new_client(schema)
    sid = _insert(c, "sessions", {"expiresAt": 9_000})  # already past 10_000

    removed = c._reap_ttl(10_000)
    assert removed == 1
    assert ("sessions", sid) not in c._docs  # physically gone — force_hard


def test_reaper_cascades_when_on_delete_children_exist() -> None:
    schema = _schema(
        {
            "sessions": {
                "fields": {"expiresAt": {"type": "number"}},
                "indexes": [{"name": "by_expires", "fields": ["expiresAt"]}],
                "ttl": {"field": "expiresAt"},
            },
            "events": {
                "fields": {
                    "tag": {"type": "string"},
                    "sessionId": {"type": "id", "table": "sessions", "onDelete": "cascade"},
                },
                "indexes": [{"name": "by_session", "fields": ["sessionId"]}],
                "softDelete": True,
            },
        }
    )
    c = _new_client(schema)
    sid = _insert(c, "sessions", {"expiresAt": 9_000})
    eid = _insert(c, "events", {"tag": "e", "sessionId": sid})

    removed = c._reap_ttl(10_000)
    assert removed == 1
    assert ("sessions", sid) not in c._docs  # hard-deleted (reaper path)
    # force_hard PROPAGATES through the cascade (server passes it to every
    # recursive child delete) — even the softDelete child is physically removed.
    assert ("events", eid) not in c._docs


def test_reaper_skips_failing_row_and_retries_next_sweep() -> None:
    """A restrict child blocks one expiry; the sweep continues (other rows go)
    and the blocked row retries on the next sweep."""
    schema = _schema(
        {
            "sessions": {
                "fields": {"expiresAt": {"type": "number"}},
                "indexes": [{"name": "by_expires", "fields": ["expiresAt"]}],
                "ttl": {"field": "expiresAt"},
            },
            "guards": {
                "fields": {
                    "tag": {"type": "string"},
                    "sessionId": {"type": "id", "table": "sessions", "onDelete": "restrict"},
                },
                "indexes": [{"name": "by_session", "fields": ["sessionId"]}],
            },
        }
    )
    c = _new_client(schema)
    s1 = _insert(c, "sessions", {"expiresAt": 9_000})
    _insert(c, "guards", {"tag": "g", "sessionId": s1})
    s2 = _insert(c, "sessions", {"expiresAt": 9_500})  # no guard — expires cleanly

    assert c._reap_ttl(10_000) == 1  # only s2; s1's cascade raised and was skipped
    assert ("sessions", s1) in c._docs
    assert ("sessions", s2) not in c._docs

    # Remove the guard: the next sweep retries and succeeds.
    g = c.collect_all("guards")[0]["_id"]
    c.mutate(Mutation.builder().delete("guards", str(g)).build())
    assert c._reap_ttl(10_000) == 1
    assert ("sessions", s1) not in c._docs


# --- push-time onDelete / softDelete validation -------------------------------


def _ref_table(table: str, on_delete: str, *, optional: bool = False) -> dict[str, Any]:
    id_ty: dict[str, Any] = {"type": "id", "table": table, "onDelete": on_delete}
    return {"type": "optional", "inner": id_ty} if optional else id_ty


def test_on_delete_on_nested_id_rejected() -> None:
    schema = _schema(
        {
            "targets": _USERS,
            "holders": {
                "fields": {
                    "meta": {
                        "type": "object",
                        "fields": {
                            "ref": {"type": "id", "table": "targets", "onDelete": "cascade"}
                        },
                    }
                },
                "indexes": [],
            },
        }
    )
    with pytest.raises(RtDbError) as ei:
        _new_client(schema)
    assert ei.value.code == ErrorCode.SCHEMA_VIOLATION
    assert ei.value.message == (
        "field 'meta' on table 'holders': onDelete is legal only on a top-level id "
        "or optional-id field"
    )


def test_on_delete_set_null_requires_optional() -> None:
    schema = _schema(
        {
            "targets": _USERS,
            "holders": {
                "fields": {"ref": _ref_table("targets", "setNull")},
                "indexes": [{"name": "by_ref", "fields": ["ref"]}],
            },
        }
    )
    with pytest.raises(RtDbError) as ei:
        _new_client(schema)
    assert ei.value.code == ErrorCode.SCHEMA_VIOLATION
    assert ei.value.message == (
        "field 'ref' on table 'holders': onDelete 'setNull' requires the id field to be optional"
    )


def test_on_delete_requires_plain_single_field_btree_index() -> None:
    base: dict[str, Any] = {
        "fields": {"label": {"type": "string"}, "ref": _ref_table("targets", "cascade")},
    }
    cases: list[tuple[dict[str, Any], str]] = [
        ({"indexes": []}, "no index"),
        (
            {"indexes": [{"name": "by_label_ref", "fields": ["label", "ref"]}]},
            "two-field index",
        ),
        (
            {"indexes": [{"name": "by_ref", "fields": ["ref"], "unique": True}]},
            "unique index",
        ),
        (
            {
                "indexes": [
                    {
                        "name": "by_ref",
                        "fields": ["ref"],
                        "where": {"field": "label", "op": "eq", "value": "x"},
                    }
                ]
            },
            "partial index",
        ),
    ]
    for indexes, why in cases:
        schema = _schema({"targets": _USERS, "holders": {**base, **indexes}})
        with pytest.raises(RtDbError) as ei:
            _new_client(schema)
        assert ei.value.code == ErrorCode.SCHEMA_VIOLATION, why
        assert ei.value.message == (
            "onDelete field 'ref' on table 'holders' requires a single-field, "
            "non-unique, non-partial btree index on it"
        ), why


def test_on_delete_unknown_referenced_table_rejected() -> None:
    schema = _schema(
        {
            "holders": {
                "fields": {"ref": _ref_table("ghosts", "cascade")},
                "indexes": [{"name": "by_ref", "fields": ["ref"]}],
            },
        }
    )
    with pytest.raises(RtDbError) as ei:
        _new_client(schema)
    assert ei.value.code == ErrorCode.SCHEMA_VIOLATION
    assert ei.value.message == (
        "onDelete field 'ref' on table 'holders' references unknown table 'ghosts'"
    )


def test_on_delete_self_reference_is_legal() -> None:
    schema = _schema(
        {
            "nodes": {
                "fields": {
                    "label": {"type": "string"},
                    "parentId": _ref_table("nodes", "cascade", optional=True),
                },
                "indexes": [{"name": "by_parent", "fields": ["parentId"]}],
            },
        }
    )
    _new_client(schema)  # does not raise


# --- additive schema changes ---------------------------------------------------


def test_adding_on_delete_and_soft_delete_is_non_destructive() -> None:
    """``onDelete`` is stripped from the type signature (server
    ``strip_on_delete``) and ``softDelete`` is a table flag — adding either to
    an existing schema pushes cleanly."""
    c = _new_client(
        _schema(
            {
                "users": _USERS,
                "posts": {
                    "fields": {
                        "title": {"type": "string"},
                        "authorId": {"type": "id", "table": "users"},
                    },
                    "indexes": [{"name": "by_author", "fields": ["authorId"]}],
                },
            }
        )
    )
    c.push_schema(
        _schema(
            {
                "users": {**_USERS, "softDelete": True},
                "posts": {
                    "fields": {
                        "title": {"type": "string"},
                        "authorId": {"type": "id", "table": "users", "onDelete": "cascade"},
                    },
                    "indexes": [{"name": "by_author", "fields": ["authorId"]}],
                },
            }
        )
    )
    # Existing rows survive the re-push.
    uid = _insert(c, "users", {"name": "u"})
    pid = _insert(c, "posts", {"title": "p", "authorId": uid})
    c.mutate(Mutation.builder().delete("users", uid).build())
    # softDelete is now live: the delete STAMPS the user...
    assert c._docs[("users", uid)].deleted_at is not None
    # ...and a soft delete never triggers a cascade (onDelete now live or not).
    assert c.get("posts", pid) is not None
