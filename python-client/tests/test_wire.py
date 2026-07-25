"""Wire-shape tests for par_rt_db.wire leaf types.

These mirror the on-the-wire JSON shapes defined by:
  - server/src/protocol.rs  (AuthedUser, ScheduleWhen, ScheduleInfo)
  - server/src/query.rs      (FilterExpr, SearchQuery, VectorSearchQuery)
and cross-checked against ts-client/src/protocol.ts and rust-client/src/wire.rs.

Discriminator field names are load-bearing: FilterExpr is tagged by ``op`` (not
``type``), and ``VectorSearchQuery.filter`` is an eq-map (``Record<string, unknown>``),
not a nested ``FilterExpr``.
"""

import pytest
from pydantic import TypeAdapter, ValidationError

from par_rt_db.wire import (
    AuthedUser,
    ClientMessage,
    FilterExpr,
    ScheduleInfo,
    ScheduleWhen,
    SearchQuery,
    ServerMessage,
    VectorSearchQuery,
)


def test_authed_user_minimal_includes_null_email_name_omits_github() -> None:
    u = AuthedUser.model_validate({"kind": "user"})
    dumped = u.model_dump(by_alias=True, mode="json")
    assert dumped["kind"] == "user"
    assert "email" in dumped and dumped["email"] is None  # null on wire
    assert "name" in dumped and dumped["name"] is None  # null on wire
    assert "githubLogin" not in dumped  # omitted when absent
    assert "githubId" not in dumped


def test_authed_user_full() -> None:
    u = AuthedUser.model_validate(
        {
            "kind": "machine",
            "email": "a@b.com",
            "name": "A",
            "githubLogin": "oct",
            "githubId": 7,
        }
    )
    assert u.model_dump(by_alias=True, mode="json") == {
        "kind": "machine",
        "email": "a@b.com",
        "name": "A",
        "githubLogin": "oct",
        "githubId": 7,
    }


def test_authed_user_rejects_unknown() -> None:
    with pytest.raises(ValidationError):
        AuthedUser.model_validate({"kind": "user", "bogus": 1})


def test_schedule_when_variants() -> None:
    adapter = TypeAdapter(ScheduleWhen)
    assert adapter.validate_python({"type": "afterMs", "ms": 5}).model_dump(
        by_alias=True, mode="json"
    ) == {"type": "afterMs", "ms": 5}
    assert adapter.validate_python({"type": "runAt", "ms": 9}).model_dump(
        by_alias=True, mode="json"
    ) == {"type": "runAt", "ms": 9}
    assert adapter.validate_python({"type": "cron", "expr": "*/5 * * * *"}).model_dump(
        by_alias=True, mode="json"
    ) == {"type": "cron", "expr": "*/5 * * * *"}


def test_schedule_info_omits_optional_when_absent() -> None:
    si = ScheduleInfo.model_validate(
        {
            "id": "j1",
            "kind": "oneshot",
            "dueAt": 100,
            "status": "pending",
            "createdAt": 1,
            "firedCount": 0,
        }
    )
    d = si.model_dump(by_alias=True, mode="json")
    assert d["id"] == "j1" and d["dueAt"] == 100 and d["firedCount"] == 0
    assert "cron" not in d and "lastError" not in d


def test_filter_expr_leaves_and_combinators() -> None:
    adapter = TypeAdapter(FilterExpr)
    eq = adapter.validate_python({"op": "eq", "field": "status", "value": "active"})
    assert eq.model_dump(by_alias=True, mode="json") == {
        "op": "eq",
        "field": "status",
        "value": "active",
    }
    inv = adapter.validate_python({"op": "in", "field": "status", "values": ["a", "b"]})
    assert inv.model_dump(by_alias=True, mode="json") == {
        "op": "in",
        "field": "status",
        "values": ["a", "b"],
    }
    and_ = adapter.validate_python(
        {
            "op": "and",
            "exprs": [
                {"op": "eq", "field": "a", "value": 1},
                {"op": "or", "exprs": []},
            ],
        }
    )
    dumped = and_.model_dump(by_alias=True, mode="json")
    assert dumped["op"] == "and"
    assert dumped["exprs"][0] == {"op": "eq", "field": "a", "value": 1}
    assert dumped["exprs"][1] == {"op": "or", "exprs": []}


def test_search_query_shape() -> None:
    sq = SearchQuery.model_validate({"index": "idx", "query": "hello"})
    assert sq.model_dump(by_alias=True, mode="json") == {"index": "idx", "query": "hello"}


def test_vector_search_query_omits_filter_when_none() -> None:
    vq = VectorSearchQuery.model_validate({"index": "v", "vector": [0.1, 0.2], "limit": 8})
    out = vq.model_dump(by_alias=True, mode="json")
    assert out["index"] == "v" and out["vector"] == [0.1, 0.2] and out["limit"] == 8
    assert "filter" not in out


def test_vector_search_query_omits_filter_when_empty() -> None:
    # Server uses BTreeMap::is_empty — empty filter must also drop.
    vq = VectorSearchQuery.model_validate({"index": "v", "vector": [0.1], "limit": 1, "filter": {}})
    assert "filter" not in vq.model_dump(by_alias=True, mode="json")


def test_vector_search_query_keeps_filter_when_non_empty() -> None:
    vq = VectorSearchQuery.model_validate(
        {"index": "v", "vector": [0.1], "limit": 1, "filter": {"status": "active"}}
    )
    out = vq.model_dump(by_alias=True, mode="json")
    assert out["filter"] == {"status": "active"}


# --- ClientMessage union (client -> server WS vocabulary) ---
#
# ClientMessage is an ``Annotated`` discriminated-union alias, so validation goes
# through a ``TypeAdapter`` (same pattern as ScheduleWhen/FilterExpr above) — the
# alias itself has no ``model_validate``.

_client_adapter = TypeAdapter(ClientMessage)


def _model(d):
    return _client_adapter.validate_python(d).model_dump(
        by_alias=True, mode="json", exclude_unset=False
    )


def test_client_auth():
    assert _model({"type": "auth", "token": "t", "db": "d"}) == {
        "type": "auth",
        "token": "t",
        "db": "d",
    }


def test_client_unsubscribe():
    assert _model({"type": "unsubscribe", "queryId": "q1"}) == {
        "type": "unsubscribe",
        "queryId": "q1",
    }


def test_client_mutate_omits_idempotency_key_when_none():
    m = _client_adapter.validate_python({"type": "mutate", "mutId": "m1", "txn": {"steps": []}})
    dumped = m.model_dump(by_alias=True, mode="json")
    assert dumped == {"type": "mutate", "mutId": "m1", "txn": {"steps": []}}
    assert "idempotencyKey" not in dumped


def test_client_mutate_with_idempotency_key():
    dumped = _client_adapter.validate_python(
        {"type": "mutate", "mutId": "m1", "idempotencyKey": "k1", "txn": {"steps": []}}
    ).model_dump(by_alias=True, mode="json")
    assert dumped == {"type": "mutate", "mutId": "m1", "idempotencyKey": "k1", "txn": {"steps": []}}


def test_client_schedule():
    dumped = _client_adapter.validate_python(
        {
            "type": "schedule",
            "scheduleId": "s1",
            "when": {"type": "afterMs", "ms": 100},
            "txn": {"steps": []},
        }
    ).model_dump(by_alias=True, mode="json")
    assert dumped == {
        "type": "schedule",
        "scheduleId": "s1",
        "when": {"type": "afterMs", "ms": 100},
        "txn": {"steps": []},
    }


def test_client_cancel_pause_resume_carry_id():
    for tag in ("cancelSchedule", "pauseSchedule", "resumeSchedule"):
        d = {"type": tag, "scheduleId": "s1", "id": "job-9"}
        assert _model(d) == d


def test_client_list_schedules():
    assert _model({"type": "listSchedules", "scheduleId": "s1"}) == {
        "type": "listSchedules",
        "scheduleId": "s1",
    }


def test_client_ping():
    assert _model({"type": "ping"}) == {"type": "ping"}


def test_client_message_rejects_unknown_fields():
    with pytest.raises(ValidationError):
        _client_adapter.validate_python({"type": "auth", "token": "t", "db": "d", "bogus": True})


# --- ServerMessage union (server -> client WS vocabulary) ---
#
# ServerMessage is an ``Annotated`` discriminated-union alias, so validation goes
# through a ``TypeAdapter`` (same pattern as ClientMessage above) — the alias
# itself has no ``model_validate``. Embedded errors are ``{code, message}``
# envelopes (not RtDbError, which is an Exception).

_server_adapter = TypeAdapter(ServerMessage)


def _server_model(d):
    return _server_adapter.validate_python(d).model_dump(
        by_alias=True, mode="json", exclude_unset=False
    )


def test_server_auth_ok():
    dumped = _server_model({"type": "authOk", "user": {"kind": "user"}})
    assert dumped["type"] == "authOk"
    assert dumped["user"] == {"kind": "user", "email": None, "name": None}


def test_server_query_update():
    assert _server_model(
        {"type": "queryUpdate", "queryId": "q1", "result": []}
    ) == {"type": "queryUpdate", "queryId": "q1", "result": []}


def test_server_mutate_err_embeds_envelope():
    assert _server_model(
        {
            "type": "mutateErr",
            "mutId": "m1",
            "error": {"code": "NOT_FOUND", "message": "x"},
        }
    ) == {
        "type": "mutateErr",
        "mutId": "m1",
        "error": {"code": "NOT_FOUND", "message": "x"},
    }


def test_server_schedule_ack_ok_omits_error():
    dumped = _server_model(
        {"type": "scheduleAck", "scheduleId": "s1", "ok": True}
    )
    assert dumped == {"type": "scheduleAck", "scheduleId": "s1", "ok": True}
    assert "error" not in dumped


def test_server_schedule_ack_err_includes_error():
    dumped = _server_model(
        {
            "type": "scheduleAck",
            "scheduleId": "s1",
            "ok": False,
            "error": {"code": "NOT_FOUND", "message": "no job"},
        }
    )
    assert dumped["ok"] is False
    assert dumped["error"] == {"code": "NOT_FOUND", "message": "no job"}


def test_server_list_schedules_ok():
    assert _server_model(
        {"type": "listSchedulesOk", "scheduleId": "s1", "schedules": []}
    ) == {"type": "listSchedulesOk", "scheduleId": "s1", "schedules": []}


def test_server_pong():
    assert _server_model({"type": "pong"}) == {"type": "pong"}


def test_server_message_rejects_unknown_fields():
    with pytest.raises(ValidationError):
        _server_adapter.validate_python(
            {"type": "pong", "bogus": True}
        )


def test_server_mutate_ok_results_passthrough():
    # ``results`` is opaque JSON until Task 9 wires the QueryResult parser.
    dumped = _server_model(
        {
            "type": "mutateOk",
            "mutId": "m1",
            "results": [{"id": "a"}, {"id": "b"}],
        }
    )
    assert dumped == {
        "type": "mutateOk",
        "mutId": "m1",
        "results": [{"id": "a"}, {"id": "b"}],
    }


def test_server_auth_err():
    dumped = _server_model(
        {"type": "authErr", "error": {"code": "UNAUTHORIZED", "message": "bad token"}}
    )
    assert dumped == {
        "type": "authErr",
        "error": {"code": "UNAUTHORIZED", "message": "bad token"},
    }


def test_server_subscribe_err():
    dumped = _server_model(
        {
            "type": "subscribeErr",
            "queryId": "q1",
            "error": {"code": "BAD_REQUEST", "message": "nope"},
        }
    )
    assert dumped == {
        "type": "subscribeErr",
        "queryId": "q1",
        "error": {"code": "BAD_REQUEST", "message": "nope"},
    }


def test_server_schedule_ok():
    dumped = _server_model(
        {"type": "scheduleOk", "scheduleId": "s1", "id": "job-9"}
    )
    assert dumped == {"type": "scheduleOk", "scheduleId": "s1", "id": "job-9"}


def test_server_schedule_err():
    dumped = _server_model(
        {
            "type": "scheduleErr",
            "scheduleId": "s1",
            "error": {"code": "NOT_FOUND", "message": "missing"},
        }
    )
    assert dumped == {
        "type": "scheduleErr",
        "scheduleId": "s1",
        "error": {"code": "NOT_FOUND", "message": "missing"},
    }
