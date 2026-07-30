"""Opt-in live-server test for the reactive WS client.

Gated on RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY. Run::

    make dev-db-up   # Postgres on 55434
    # start the server on :8300 with RTDB_ADMIN_KEY=dev-admin-key
    RTDB_TEST_SERVER_URL=http://127.0.0.1:8300 \\
    RTDB_TEST_ADMIN_KEY=dev-admin-key \\
    uv run pytest tests/test_ws_integration.py -q -m live
"""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from par_rt_db.ws_client import Subscription

pytestmark = pytest.mark.skipif(
    not (os.environ.get("RTDB_TEST_SERVER_URL") and os.environ.get("RTDB_TEST_ADMIN_KEY")),
    reason="set RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY to run live WS tests",
)


@pytest.mark.live
async def test_subscribe_and_live_update() -> None:
    from par_rt_db import Mutation, SchemaDef, TableQuery
    from par_rt_db.http_client import RtDbHttpClient
    from par_rt_db.ws_client import RtDbClient

    url = os.environ["RTDB_TEST_SERVER_URL"]
    admin_key = os.environ["RTDB_TEST_ADMIN_KEY"]
    db = "t" + uuid.uuid4().hex[:12]

    schema = SchemaDef.model_validate({"tables": {"items": {"fields": {"n": {"type": "number"}}}}})
    admin = RtDbHttpClient(url, db, admin_key)
    admin.create_db(db)
    admin.push_schema(db, schema)
    token = admin.mint_token(db, "test").token

    try:

        async def get_token() -> str | None:
            return token

        client = RtDbClient(url, db, get_token=get_token)
        await client.connect()
        sub = client.subscribe(TableQuery("items").collect())
        try:
            # Wait for the initial (empty) push.
            await asyncio.wait_for(_first(sub), 10.0)
            assert sub.current() == []
            # Insert over WS and await the live update.
            # ``_id`` is a server-managed system field (reserved); insert user
            # data only and let the server assign the id.
            await client.mutate(Mutation.builder().insert("items", {"n": 1}).build())
            await asyncio.wait_for(_next_nonempty(sub), 10.0)
            docs = sub.current()
            assert docs is not None
            assert any(d.get("n") == 1 for d in docs)
        finally:
            await client.close()
    finally:
        admin.delete_db(db, db)


async def _first(sub: Subscription) -> object:
    async for value in sub:
        return value


async def _next_nonempty(sub: Subscription) -> object:
    async for value in sub:
        if value:
            return value
