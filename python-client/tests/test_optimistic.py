"""Tests for optimistic updates: the pure projection and the WS wiring.

The pure cases (``project``) mirror ``rust-client/src/optimistic.rs``'s test
suite — unfiltered collect (insert/patch/replace/delete), filtered collect
(delete-only), ``get(id)`` point read, and the decline rules (upsert, the
rank/membership-dependent terminals, full take window, no-op patch). The wiring
cases exercise the apply/reconcile/rollback hooks via the no-socket ``FakeConn``
harness shared with ``test_ws_routing``.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from par_rt_db import Mutation, TableQuery
from par_rt_db.optimistic import project
from par_rt_db.query import _dump_query
from par_rt_db.ws_client import RtDbClient, RtDbError

from .test_ws_routing import FakeConn, _drain, _id, _qid, make_client

# --- pure-projection helpers ------------------------------------------------


def _q(tq: TableQuery) -> dict[str, Any]:
    """Wire dict for a TableQuery (the projection's ``query_dict`` input)."""
    return _dump_query(tq)


def _steps(builder: Any) -> list[dict[str, Any]]:
    """Wire step list from a MutationBuilder (the projection's ``txn_steps``)."""
    return builder.build().model_dump(by_alias=True, mode="json")["steps"]


def _collect() -> dict[str, Any]:
    return _q(TableQuery("items").collect())


# --- pure: unfiltered collect (insert/patch/replace/delete) ------------------


def test_insert_overlays_on_unfiltered_collect():
    last = [{"_id": "a", "_creationTime": 1, "_version": 1, "title": "x"}]
    steps = _steps(Mutation.builder().insert("items", {"title": "y"}))
    value, did = project(_collect(), last, steps, 99)
    assert did
    assert isinstance(value, list)
    assert len(value) == 2
    overlaid = value[1]
    assert isinstance(overlaid["_id"], str)
    assert overlaid["_id"].startswith("__optimistic__")
    assert overlaid["_creationTime"] == 99
    assert overlaid["_version"] == 1
    assert overlaid["title"] == "y"
    # The original doc is untouched (no mutation of `last`).
    assert last == [{"_id": "a", "_creationTime": 1, "_version": 1, "title": "x"}]


def test_patch_overlays_by_id():
    last = [{"_id": "a", "_creationTime": 1, "_version": 1, "n": 1}]
    steps = _steps(Mutation.builder().patch("items", "a", {"n": 2}))
    value, did = project(_collect(), last, steps, 99)
    assert did
    assert isinstance(value, list)
    assert value[0]["n"] == 2
    assert value[0]["_id"] == "a"


def test_replace_overlays_by_id_preserving_system_fields():
    last = [{"_id": "a", "_creationTime": 7, "_version": 3, "n": 1, "old": True}]
    steps = _steps(Mutation.builder().replace("items", "a", {"n": 2, "new": True}))
    value, did = project(_collect(), last, steps, 99)
    assert did
    doc = value[0]
    # _id and _creationTime come from the old doc; _version is dropped; body is the new doc.
    assert doc["_id"] == "a"
    assert doc["_creationTime"] == 7
    assert "_version" not in doc
    assert doc["n"] == 2
    assert doc["new"] is True
    assert "old" not in doc


def test_delete_overlays_by_id():
    last = [
        {"_id": "a", "_creationTime": 1, "_version": 1},
        {"_id": "b", "_creationTime": 2, "_version": 1},
    ]
    steps = _steps(Mutation.builder().delete("items", "a"))
    value, did = project(_collect(), last, steps, 99)
    assert did
    assert isinstance(value, list)
    assert len(value) == 1
    assert value[0]["_id"] == "b"


def test_noop_patch_returns_skip():
    # A patch that sets a field to its existing value is a no-op (canonical equality).
    last = [{"_id": "a", "_creationTime": 1, "_version": 1, "n": 1}]
    steps = _steps(Mutation.builder().patch("items", "a", {"n": 1}))
    value, did = project(_collect(), last, steps, 99)
    assert not did
    assert value is None


def test_step_on_different_table_is_skipped():
    last = [{"_id": "a", "_creationTime": 1, "_version": 1}]
    steps = _steps(Mutation.builder().insert("other", {"x": 1}))
    value, did = project(_collect(), last, steps, 99)
    assert not did


def test_multi_step_insert_then_delete_overlays():
    last = [{"_id": "a", "_creationTime": 1, "_version": 1}]
    steps = _steps(Mutation.builder().insert("items", {"n": 2}).delete("items", "a"))
    value, did = project(_collect(), last, steps, 99)
    assert did
    assert isinstance(value, list)
    # `a` deleted, the synthetic insert remains.
    assert len(value) == 1
    assert isinstance(value[0]["_id"], str)
    assert value[0]["_id"].startswith("__optimistic__")


def test_does_not_mutate_last_value():
    last = [{"_id": "a", "_creationTime": 1, "_version": 1, "n": 1}]
    snapshot = json.loads(json.dumps(last))
    _ = project(_collect(), last, _steps(Mutation.builder().patch("items", "a", {"n": 9})), 5)
    assert last == snapshot


# --- pure: take window + synthetic id uniqueness -----------------------------


def test_insert_skips_when_take_window_full():
    q = _q(TableQuery("items").take(1))
    last = [{"_id": "a", "_creationTime": 1, "_version": 1}]
    steps = _steps(Mutation.builder().insert("items", {"title": "y"}))
    value, did = project(q, last, steps, 99)
    assert not did


def test_insert_overlays_when_take_window_not_full():
    q = _q(TableQuery("items").take(2))
    last = [{"_id": "a", "_creationTime": 1, "_version": 1}]
    steps = _steps(Mutation.builder().insert("items", {"title": "y"}))
    value, did = project(q, last, steps, 99)
    assert did
    assert len(value) == 2


def test_synthetic_ids_are_unique():
    last: list[Any] = []
    steps_a = _steps(Mutation.builder().insert("items", {"title": "a"}))
    steps_b = _steps(Mutation.builder().insert("items", {"title": "b"}))
    va, _ = project(_collect(), last, steps_a, 1)
    vb, _ = project(_collect(), last, steps_b, 2)
    assert isinstance(va, list) and isinstance(vb, list)
    id_a = va[0]["_id"]
    id_b = vb[0]["_id"]
    assert id_a.startswith("__optimistic__")
    assert id_b.startswith("__optimistic__")
    assert id_a != id_b


# --- pure: filtered array (delete-only) --------------------------------------


def test_filtered_array_delete_only():
    q = _q(TableQuery("items").with_index("by_status").eq("active").collect())
    last = [{"_id": "a", "_creationTime": 1, "_version": 1}]
    # delete overlays.
    del_steps = _steps(Mutation.builder().delete("items", "a"))
    value, did = project(q, last, del_steps, 99)
    assert did
    assert value == []
    # insert declines (membership-ambiguous under the filter).
    ins_steps = _steps(Mutation.builder().insert("items", {"title": "y"}))
    value, did = project(q, last, ins_steps, 99)
    assert not did


def test_filter_predicate_treated_as_filtered_array():
    # Gap-fix (rust): a collect with a `filter` predicate routes to delete-only
    # projection, not unfiltered-array. Delete overlays; insert skips. Built as a
    # raw wire dict — the projection only checks `filter` is present, and
    # `FilterExpr` is an Annotated alias (no `.model_validate`).
    q = {"table": "items", "filter": {"op": "eq", "field": "status", "value": "done"}}
    last = [
        {"_id": "a", "_creationTime": 1, "_version": 1},
        {"_id": "b", "_creationTime": 2, "_version": 1},
    ]
    del_steps = _steps(Mutation.builder().delete("items", "a"))
    value, did = project(q, last, del_steps, 99)
    assert did
    assert len(value) == 1
    ins_steps = _steps(Mutation.builder().insert("items", {"title": "y"}))
    value, did = project(q, last, ins_steps, 99)
    assert not did


# --- pure: get(id) point read ------------------------------------------------


def _get_query(target: str) -> dict[str, Any]:
    return _q(TableQuery("items").get(target))


def test_get_patch_overlays():
    q = _get_query("a")
    last = {"_id": "a", "_creationTime": 1, "_version": 1, "n": 1}
    steps = _steps(Mutation.builder().patch("items", "a", {"n": 2}))
    value, did = project(q, last, steps, 99)
    assert did
    assert value["n"] == 2
    assert value["_id"] == "a"
    assert value["_creationTime"] == 1


def test_get_delete_overlays_to_null():
    q = _get_query("a")
    last = {"_id": "a", "_creationTime": 1, "_version": 1, "n": 1}
    steps = _steps(Mutation.builder().delete("items", "a"))
    value, did = project(q, last, steps, 99)
    assert did
    assert value is None


def test_get_replace_preserves_system_fields():
    q = _get_query("a")
    last = {"_id": "a", "_creationTime": 7, "_version": 3, "n": 1}
    steps = _steps(Mutation.builder().replace("items", "a", {"n": 2}))
    value, did = project(q, last, steps, 99)
    assert did
    assert value["n"] == 2
    assert value["_id"] == "a"
    assert value["_creationTime"] == 7
    assert "_version" not in value


def test_get_patch_of_other_id_is_noop():
    q = _get_query("a")
    last = {"_id": "a", "_creationTime": 1, "_version": 1, "n": 1}
    steps = _steps(Mutation.builder().patch("items", "b", {"n": 2}))
    value, did = project(q, last, steps, 99)
    assert not did


def test_get_with_null_last_skips():
    q = _get_query("a")
    steps = _steps(Mutation.builder().patch("items", "a", {"n": 2}))
    value, did = project(q, None, steps, 99)
    assert not did


# --- pure: declines (upsert + rank/membership terminals) ---------------------


def test_upsert_always_skips_on_unfiltered_collect():
    last = [{"_id": "a", "_creationTime": 1, "_version": 1}]
    steps = _steps(Mutation.builder().upsert("items", "by_n", [1], {"n": 1}, {"n": 2}))
    value, did = project(_collect(), last, steps, 99)
    assert not did


def test_rank_and_membership_terminals_all_skip():
    last = [{"_id": "a", "_creationTime": 1, "_version": 1}]
    steps = _steps(
        Mutation.builder()
        .insert("items", {"title": "y"})
        .patch("items", "a", {"n": 2})
        .delete("items", "a")
    )
    terminals = [
        _q(TableQuery("items").with_index("by_status").eq("active").unique()),
        _q(TableQuery("items").first()),
        _q(TableQuery("items").count()),
        _q(TableQuery("items").distinct()),
        _q(TableQuery("items").paginate(num_items=10)),
        _q(TableQuery("items").search("search_idx", "query").take(5)),
        _q(TableQuery("items").vector_search("vec_idx", [1.0, 0.0], limit=5).take(5)),
    ]
    for q in terminals:
        value, did = project(q, last, steps, 99)
        assert not did, f"terminal query should skip: {q}"


def test_non_array_last_skips():
    # An unfiltered-array projection against a non-list last value declines.
    last = {"not": "a list"}
    steps = _steps(Mutation.builder().insert("items", {"title": "y"}))
    value, did = project(_collect(), last, steps, 99)
    assert not did


# --- WS wiring: apply / reconcile / rollback ---------------------------------
#
# Reuses the no-socket FakeConn / make_client / _drain harness from
# test_ws_routing. These tests construct the client with optimistic_updates=True.


async def _connected_optimistic(conn: FakeConn) -> RtDbClient:
    """Connect a client with optimistic updates on and complete the auth handshake."""
    client, _ = make_client(conn, optimistic_updates=True)
    await client.connect()
    await _drain()
    await conn.deliver('{"type":"authOk","user":{"kind":"machine"}}')
    await _drain()
    return client


async def test_overlay_applied_on_mutate_then_reconciled_by_query_update():
    conn = FakeConn()
    client = await _connected_optimistic(conn)
    try:
        sub = client.subscribe(TableQuery("items").collect())
        await _drain()
        qid = _qid(conn, "subscribe")
        # Seed the projection base with an authoritative result.
        await conn.deliver(
            '{"type":"queryUpdate","queryId":"'
            + qid
            + '","result":[{"_id":"a","_creationTime":1,"_version":1,"n":1}]}'
        )
        await _drain()
        assert sub.current() == [{"_id": "a", "_creationTime": 1, "_version": 1, "n": 1}]

        task = asyncio.create_task(
            client.mutate(Mutation.builder().insert("items", {"n": 2}).build())
        )
        await _drain()
        # Overlay applied BEFORE the server replies.
        cur = sub.current()
        assert isinstance(cur, list)
        assert len(cur) == 2
        assert cur[1]["n"] == 2
        assert isinstance(cur[1]["_id"], str)
        assert cur[1]["_id"].startswith("__optimistic__")

        mid = _id(conn, "mutate")
        # mutateOk drops the reverse-index entry but does NOT revert — the
        # reconciling queryUpdate will supersede.
        await conn.deliver('{"type":"mutateOk","mutId":"' + mid + '","results":[{"id":"srv1"}]}')
        await _drain()
        cur = sub.current()
        assert isinstance(cur, list) and len(cur) == 2  # still overlaid

        # Authoritative queryUpdate reconciles (server-wins).
        await conn.deliver(
            '{"type":"queryUpdate","queryId":"'
            + qid
            + '","result":[{"_id":"a","_creationTime":1,"_version":1,"n":1},'
            '{"_id":"srv1","_creationTime":50,"_version":1,"n":2}]}'
        )
        await _drain()
        cur = sub.current()
        assert isinstance(cur, list)
        assert len(cur) == 2
        assert cur[1]["_id"] == "srv1"  # real server id replaces the synthetic one

        results = await asyncio.wait_for(task, 1.0)
        assert results[0] is not None
        assert results[0].model_dump()["id"] == "srv1"
    finally:
        await client.close()


async def test_overlay_rolled_back_on_mutate_err():
    conn = FakeConn()
    client = await _connected_optimistic(conn)
    try:
        sub = client.subscribe(TableQuery("items").collect())
        await _drain()
        qid = _qid(conn, "subscribe")
        base = [{"_id": "a", "_creationTime": 1, "_version": 1, "n": 1}]
        await conn.deliver(
            '{"type":"queryUpdate","queryId":"' + qid + '","result":' + json.dumps(base) + "}"
        )
        await _drain()

        task = asyncio.create_task(
            client.mutate(Mutation.builder().insert("items", {"n": 2}).build())
        )
        await _drain()
        cur = sub.current()
        assert isinstance(cur, list) and len(cur) == 2  # overlaid

        mid = _id(conn, "mutate")
        await conn.deliver(
            '{"type":"mutateErr","mutId":"'
            + mid
            + '","error":{"code":"INTERNAL","message":"boom"}}'
        )
        await _drain()
        # Rolled back to the authoritative base.
        assert sub.current() == base

        with pytest.raises(RtDbError):
            await asyncio.wait_for(task, 1.0)
    finally:
        await client.close()


async def test_overlay_rolled_back_on_inflight_drop():
    conn = FakeConn()
    client = await _connected_optimistic(conn)
    try:
        sub = client.subscribe(TableQuery("items").collect())
        await _drain()
        qid = _qid(conn, "subscribe")
        base = [{"_id": "a", "_creationTime": 1, "_version": 1, "n": 1}]
        await conn.deliver(
            '{"type":"queryUpdate","queryId":"' + qid + '","result":' + json.dumps(base) + "}"
        )
        await _drain()

        inflight = asyncio.create_task(
            client.mutate(Mutation.builder().insert("items", {"n": 2}).build())
        )
        await _drain()
        cur = sub.current()
        assert isinstance(cur, list) and len(cur) == 2  # overlaid

        # Reconnectable drop: in-flight mutate is rejected and its overlay reverts.
        conn.close_code = 4000
        await conn._inbox.put(None)
        await _drain()
        assert sub.current() == base

        with pytest.raises(RtDbError):
            await asyncio.wait_for(inflight, 1.0)
    finally:
        await client.close()


async def test_get_point_read_overlay_patches():
    conn = FakeConn()
    client = await _connected_optimistic(conn)
    try:
        sub = client.subscribe(TableQuery("items").get("a"))
        await _drain()
        qid = _qid(conn, "subscribe")
        await conn.deliver(
            '{"type":"queryUpdate","queryId":"'
            + qid
            + '","result":{"_id":"a","_creationTime":1,"_version":1,"n":1}}'
        )
        await _drain()
        assert sub.current() == {"_id": "a", "_creationTime": 1, "_version": 1, "n": 1}

        task = asyncio.create_task(
            client.mutate(Mutation.builder().patch("items", "a", {"n": 2}).build())
        )
        await _drain()
        # get(id) patch overlays immediately.
        cur = sub.current()
        assert isinstance(cur, dict)
        assert cur["n"] == 2

        mid = _id(conn, "mutate")
        await conn.deliver('{"type":"mutateOk","mutId":"' + mid + '","results":[{"id":"a"}]}')
        await _drain()
        # Reconcile to the authoritative value.
        await conn.deliver(
            '{"type":"queryUpdate","queryId":"'
            + qid
            + '","result":{"_id":"a","_creationTime":1,"_version":2,"n":2}}'
        )
        await _drain()
        cur = sub.current()
        assert isinstance(cur, dict)
        assert cur["n"] == 2
        assert cur["_version"] == 2

        await asyncio.wait_for(task, 1.0)
    finally:
        await client.close()


async def test_optimistic_off_does_not_overlay():
    # Default (optimistic_updates=False): mutate must not touch sub.value before
    # the server round-trip — byte-for-byte the pre-optimistic behavior.
    conn = FakeConn()
    client, _ = make_client(conn)  # default: optimistic_updates=False
    await client.connect()
    await _drain()
    try:
        await conn.deliver('{"type":"authOk","user":{"kind":"machine"}}')
        await _drain()
        sub = client.subscribe(TableQuery("items").collect())
        await _drain()
        qid = _qid(conn, "subscribe")
        base = [{"_id": "a", "_creationTime": 1, "_version": 1, "n": 1}]
        await conn.deliver(
            '{"type":"queryUpdate","queryId":"' + qid + '","result":' + json.dumps(base) + "}"
        )
        await _drain()

        task = asyncio.create_task(
            client.mutate(Mutation.builder().insert("items", {"n": 2}).build())
        )
        await _drain()
        # No overlay — value unchanged until the server replies.
        assert sub.current() == base
        assert client._overlays == {}

        mid = _id(conn, "mutate")
        await conn.deliver('{"type":"mutateOk","mutId":"' + mid + '","results":[{"id":"srv1"}]}')
        await asyncio.wait_for(task, 1.0)
    finally:
        await client.close()
