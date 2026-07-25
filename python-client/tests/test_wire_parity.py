"""Round-trip parity: our serialized JSON must equal the server's wire bytes.

Fixtures are copied verbatim from ``server/src/protocol.rs`` tests (the
authoritative wire shapes). A failure here means a wire model drifted from the
contract.

``ClientMessage`` and ``ServerMessage`` are ``Annotated[Union, Field(discriminator=...)]``
aliases, so validation routes through ``TypeAdapter`` (the alias itself has no
``model_validate``) — same pattern as ``tests/test_wire.py``.
"""

import json

import pytest
from pydantic import TypeAdapter

from par_rt_db.wire import ClientMessage, ServerMessage

# (wire_json_string) — every entry must round-trip identically.
CLIENT_FIXTURES: list[str] = [
    '{"type": "auth", "token": "t", "db": "d"}',
    '{"type": "subscribe", "queryId": "q1", "query": {"table": "t"}}',
    '{"type": "unsubscribe", "queryId": "q1"}',
    '{"type": "mutate", "mutId": "m1", "txn": {"steps": []}}',
    '{"type": "mutate", "mutId": "m1", "idempotencyKey": "key1", "txn": {"steps": []}}',
    '{"type": "ping"}',
    (
        '{"type": "schedule", "scheduleId": "s1", '
        '"when": {"type": "afterMs", "ms": 100}, "txn": {"steps": []}}'
    ),
    '{"type": "cancelSchedule", "scheduleId": "s1", "id": "job-9"}',
    '{"type": "pauseSchedule", "scheduleId": "s1", "id": "job-9"}',
    '{"type": "resumeSchedule", "scheduleId": "s1", "id": "job-9"}',
    '{"type": "listSchedules", "scheduleId": "s1"}',
]

SERVER_FIXTURES: list[str] = [
    '{"type": "queryUpdate", "queryId": "q1", "result": []}',
    '{"type": "mutateOk", "mutId": "m1", "results": []}',
    (
        '{"type": "subscribeErr", "queryId": "q1", '
        '"error": {"code": "BAD_REQUEST", "message": "bad index"}}'
    ),
    '{"type": "pong"}',
    '{"type": "scheduleOk", "scheduleId": "s1", "id": "job-9"}',
    '{"type": "scheduleAck", "scheduleId": "s1", "ok": true}',
    (
        '{"type": "scheduleAck", "scheduleId": "s1", "ok": false, '
        '"error": {"code": "NOT_FOUND", "message": "x"}}'
    ),
]


_client_adapter = TypeAdapter(ClientMessage)
_server_adapter = TypeAdapter(ServerMessage)


@pytest.mark.parametrize("wire", CLIENT_FIXTURES)
def test_client_message_round_trip(wire: str) -> None:
    expected = json.loads(wire)
    msg = _client_adapter.validate_python(expected)
    dumped = json.loads(msg.model_dump_json(by_alias=True))
    assert dumped == expected, f"client wire drift: {dumped} != {expected}"


@pytest.mark.parametrize("wire", SERVER_FIXTURES)
def test_server_message_round_trip(wire: str) -> None:
    expected = json.loads(wire)
    msg = _server_adapter.validate_python(expected)
    dumped = json.loads(msg.model_dump_json(by_alias=True))
    assert dumped == expected, f"server wire drift: {dumped} != {expected}"
