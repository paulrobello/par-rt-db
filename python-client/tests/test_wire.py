"""Wire-shape tests for par_rt_db.wire leaf types.

These mirror the on-the-wire JSON shapes defined by:
  - server/src/protocol.rs  (AuthedUser, ScheduleWhen, ScheduleInfo)
  - server/src/query.rs      (FilterExpr, SearchQuery, VectorSearchQuery)
and cross-checked against ts-client/src/protocol.ts and rust-client/src/wire.rs.

Discriminator field names are load-bearing: FilterExpr is tagged by ``op`` (not
``type``), and ``VectorSearchQuery.filter`` is the same full ``FilterExpr`` that
``SearchQuery.filter`` is (omitted on the wire when ``None``).
"""

import pytest
from pydantic import TypeAdapter, ValidationError

from par_rt_db.wire import (
    AuthedUser,
    ClientMessage,
    FilterExpr,
    HybridSearchQuery,
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


def test_filter_expr_not_contains_exists_variants() -> None:
    """New variants mirroring server FilterExpr (Task 1, commit b6b6c2a).

    Wire shapes must match byte-for-byte: ``{"op":"not","expr":{...}}``,
    ``{"op":"contains","field","value"}``, ``{"op":"exists","field"}``.
    """
    adapter = TypeAdapter(FilterExpr)

    # `not` wraps a nested FilterExpr.
    not_expr = adapter.validate_python(
        {"op": "not", "expr": {"op": "eq", "field": "status", "value": "done"}}
    )
    assert not_expr.model_dump(by_alias=True, mode="json") == {
        "op": "not",
        "expr": {"op": "eq", "field": "status", "value": "done"},
    }

    # `contains`: value ∈ doc.field[] (reverse of `in`).
    contains = adapter.validate_python({"op": "contains", "field": "tags", "value": "red"})
    assert contains.model_dump(by_alias=True, mode="json") == {
        "op": "contains",
        "field": "tags",
        "value": "red",
    }

    # `exists`: field present and non-null.
    exists = adapter.validate_python({"op": "exists", "field": "dueAt"})
    assert exists.model_dump(by_alias=True, mode="json") == {
        "op": "exists",
        "field": "dueAt",
    }

    # Unknown fields are rejected (mirrors Rust deny_unknown_fields).
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {"op": "not", "expr": {"op": "eq", "field": "x", "value": 1}, "bogus": True}
        )
    with pytest.raises(ValidationError):
        adapter.validate_python({"op": "contains", "field": "x", "value": 1, "bogus": True})
    with pytest.raises(ValidationError):
        adapter.validate_python({"op": "exists", "field": "x", "bogus": True})


def test_search_query_shape() -> None:
    sq = SearchQuery.model_validate({"index": "idx", "query": "hello"})
    assert sq.model_dump(by_alias=True, mode="json") == {"index": "idx", "query": "hello"}


def test_search_query_omits_mode_when_none() -> None:
    # FM-30: `mode` is optional and omitted entirely when unset, so existing
    # requests stay byte-identical (the corpus `queries` section pins this).
    sq = SearchQuery.model_validate({"index": "idx", "query": "hello"})
    out = sq.model_dump(by_alias=True, mode="json")
    assert out == {"index": "idx", "query": "hello"}
    assert "mode" not in out


def test_search_query_round_trips_mode() -> None:
    for mode in ("tsquery", "trgm"):
        sq = SearchQuery.model_validate({"index": "idx", "query": "conv", "mode": mode})
        assert sq.model_dump(by_alias=True, mode="json") == {
            "index": "idx",
            "query": "conv",
            "mode": mode,
        }


def test_search_query_rejects_unknown_mode() -> None:
    # The closed domain is {"tsquery", "trgm"}; anything else is rejected at
    # parse time (the server's SearchMode enum rejects it as BadRequest).
    with pytest.raises(ValidationError):
        SearchQuery.model_validate({"index": "idx", "query": "conv", "mode": "fuzzy"})


def test_search_query_omits_snippet_when_none() -> None:
    # FM-31: `snippet` is optional and omitted entirely when unset, so existing
    # requests stay byte-identical (the corpus `queries` section pins this).
    sq = SearchQuery.model_validate({"index": "idx", "query": "hello"})
    out = sq.model_dump(by_alias=True, mode="json")
    assert out == {"index": "idx", "query": "hello"}
    assert "snippet" not in out


def test_search_query_round_trips_snippet() -> None:
    # Both wire booleans round-trip: true opts hits into _searchSnippet, and an
    # explicit false behaves like omission server-side but still serializes
    # (the server skips only None).
    for snippet in (True, False):
        sq = SearchQuery.model_validate({"index": "idx", "query": "hi", "snippet": snippet})
        assert sq.model_dump(by_alias=True, mode="json") == {
            "index": "idx",
            "query": "hi",
            "snippet": snippet,
        }


def test_search_query_snippet_composes_with_mode_and_filter() -> None:
    # The full FM-30+FM-31 shape round-trips together (tsquery is the only
    # legal mode for a snippet; trgm is a server-side BadRequest).
    sq = SearchQuery.model_validate(
        {
            "index": "idx",
            "query": '"exact phrase" or -excluded',
            "mode": "tsquery",
            "snippet": True,
            "filter": {"op": "eq", "field": "status", "value": "open"},
        }
    )
    assert sq.model_dump(by_alias=True, mode="json") == {
        "index": "idx",
        "query": '"exact phrase" or -excluded',
        "mode": "tsquery",
        "snippet": True,
        "filter": {"op": "eq", "field": "status", "value": "open"},
    }


def test_vector_search_query_omits_filter_when_none() -> None:
    vq = VectorSearchQuery.model_validate({"index": "v", "vector": [0.1, 0.2], "limit": 8})
    out = vq.model_dump(by_alias=True, mode="json")
    assert out["index"] == "v" and out["vector"] == [0.1, 0.2] and out["limit"] == 8
    assert "filter" not in out


def test_vector_search_query_keeps_full_filter_expr() -> None:
    # vectorSearch.filter is the full FilterExpr (the same type search uses);
    # it round-trips byte-identical when present.
    vq = VectorSearchQuery.model_validate(
        {
            "index": "v",
            "vector": [0.1],
            "limit": 1,
            "filter": {"op": "eq", "field": "status", "value": "active"},
        }
    )
    out = vq.model_dump(by_alias=True, mode="json")
    assert out["filter"] == {"op": "eq", "field": "status", "value": "active"}


def test_vector_search_query_rejects_eq_map_filter() -> None:
    # The old eq-map shape (a bare field→value dict) is no longer valid: filter
    # must be a tagged FilterExpr. A plain dict without an ``op`` is rejected.
    with pytest.raises(ValidationError):
        VectorSearchQuery.model_validate(
            {"index": "v", "vector": [0.1], "limit": 1, "filter": {"status": "active"}}
        )


def test_hybrid_search_query_omits_optionals_when_absent() -> None:
    hq = HybridSearchQuery.model_validate({"query": "hello", "vector": [0.1, 0.2], "limit": 5})
    out = hq.model_dump(by_alias=True, mode="json")
    assert out == {"query": "hello", "vector": [0.1, 0.2], "limit": 5}
    # camelCase aliases never leak the snake_case Python names.
    assert "search_index" not in out and "vector_index" not in out


def test_hybrid_search_query_round_trips_optionals() -> None:
    hq = HybridSearchQuery.model_validate(
        {
            "query": "hello",
            "vector": [0.1],
            "limit": 1,
            "searchIndex": "search_body",
            "vectorIndex": "by_embedding",
            "k": 42,
        }
    )
    out = hq.model_dump(by_alias=True, mode="json")
    assert out == {
        "query": "hello",
        "vector": [0.1],
        "limit": 1,
        "searchIndex": "search_body",
        "vectorIndex": "by_embedding",
        "k": 42,
    }


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
    assert _server_model({"type": "queryUpdate", "queryId": "q1", "result": []}) == {
        "type": "queryUpdate",
        "queryId": "q1",
        "result": [],
    }


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
    dumped = _server_model({"type": "scheduleAck", "scheduleId": "s1", "ok": True})
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
    assert _server_model({"type": "listSchedulesOk", "scheduleId": "s1", "schedules": []}) == {
        "type": "listSchedulesOk",
        "scheduleId": "s1",
        "schedules": [],
    }


def test_server_pong():
    assert _server_model({"type": "pong"}) == {"type": "pong"}


def test_server_message_rejects_unknown_fields():
    with pytest.raises(ValidationError):
        _server_adapter.validate_python({"type": "pong", "bogus": True})


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
    dumped = _server_model({"type": "scheduleOk", "scheduleId": "s1", "id": "job-9"})
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


# --- Migration wire shapes (tag "op", camelCase) ---


def test_migration_directive_round_trip() -> None:
    """Every directive variant round-trips with the correct ``op`` discriminator
    and camelCase wire keys. Mirrors ``server/src/migrate.rs`` tests."""
    from par_rt_db import Cast, Migration
    from par_rt_db.migration import MigrateRequest

    req = (
        Migration.builder()
        .rename_field("users", "name", "fullName")
        .rename_table("old", "new")
        .change_type("users", "age", {"type": "string"}, Cast.TO_STRING, "0")
        .drop_field("users", "legacy")
        .drop_table("gone")
        .drop_index("users", "by_email")
        .set_default("users", "role", "member")
        .eval_expr("users", "upper", "upper(doc->>'fullName')", "doc ? 'fullName'")
        .dry_run()
        .build()
    )
    dumped = req.model_dump(by_alias=True, mode="json")
    ops = [d["op"] for d in dumped["directives"]]
    assert ops == [
        "renameField",
        "renameTable",
        "changeType",
        "dropField",
        "dropTable",
        "dropIndex",
        "setDefault",
        "evalExpr",
    ]
    # `from` is a Python keyword; wire alias is `from`.
    assert dumped["directives"][0]["from"] == "name"
    assert dumped["directives"][1]["from"] == "old"
    # `where` is a Python keyword; wire alias is `where`.
    assert dumped["directives"][7]["where"] == "doc ? 'fullName'"
    # `cast` serializes as the camelCase literal.
    assert dumped["directives"][2]["cast"] == "toString"
    # `default` is present when set.
    assert dumped["directives"][2]["default"] == "0"
    # `dryRun` is camelCase.
    assert dumped["dryRun"] is True

    # Round-trip back through the model.
    parsed = MigrateRequest.model_validate(dumped)
    assert parsed.dry_run is True
    assert len(parsed.directives) == 8
    re_dumped = parsed.model_dump(by_alias=True, mode="json")
    assert re_dumped == dumped


def test_migration_directive_omits_unset_default_and_where() -> None:
    """``changeType.default`` and ``evalExpr.where`` are omitted when unset,
    matching the ts-client's omit convention."""
    from par_rt_db import Cast, Migration

    req = (
        Migration.builder()
        .change_type("users", "age", {"type": "string"}, Cast.TO_STRING)
        .eval_expr("users", "upper", "upper(doc->>'name')")
        .build()
    )
    dumped = req.model_dump(by_alias=True, mode="json")
    assert "default" not in dumped["directives"][0]
    assert "where" not in dumped["directives"][1]


def test_migration_cast_literals() -> None:
    """``Cast`` serializes as the four camelCase wire literals."""
    from par_rt_db import Cast, Migration

    for cast, wire in [
        (Cast.TO_STRING, "toString"),
        (Cast.TO_NUMBER, "toNumber"),
        (Cast.TO_INT64, "toInt64"),
        (Cast.TO_BOOLEAN, "toBoolean"),
    ]:
        req = Migration.builder().change_type("t", "f", {"type": "string"}, cast).build()
        dumped = req.model_dump(by_alias=True, mode="json")
        assert dumped["directives"][0]["cast"] == wire


def test_migration_rejects_unknown_directive_fields() -> None:
    """``extra='forbid'`` on every directive variant rejects unknown fields."""
    from pydantic import ValidationError

    from par_rt_db.migration import _RenameField

    with pytest.raises(ValidationError):
        _RenameField.model_validate(
            {"op": "renameField", "table": "t", "from": "a", "to": "b", "bogus": 1}
        )
