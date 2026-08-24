"""Guard against sync/async surface drift (QA-003).

``RtDbAdminClient``/``AsyncRtDbAdminClient`` and ``RtDbHttpClient``/
``RtDbAsyncHttpClient`` are meant to be one-to-one mirrors — every public sync
method has an async twin with the same name (ARC-108: both route through the
shared ``_op_*`` request builders in :mod:`par_rt_db.admin`, so there is only
one place new admin surface can be added). This test is the guard that a new
method added to one side and forgotten on the other doesn't silently ship —
the wire-corpus and per-method tests don't catch that class of drift.
"""

from __future__ import annotations

from par_rt_db.admin import AsyncRtDbAdminClient, RtDbAdminClient
from par_rt_db.aio_http_client import RtDbAsyncHttpClient
from par_rt_db.http_client import RtDbHttpClient

# ``aclose`` is an intentional async-idiom alias for ``close`` (PEP-533 /
# anyio convention for graceful-shutdown-on-cancellation) and has no sync
# counterpart by design.
_ASYNC_ONLY = {"aclose"}


def _public_names(cls: type) -> set[str]:
    return {name for name in dir(cls) if not name.startswith("_")}


def test_admin_client_sync_async_parity() -> None:
    sync_names = _public_names(RtDbAdminClient)
    async_names = _public_names(AsyncRtDbAdminClient) - _ASYNC_ONLY
    assert sync_names == async_names


def test_http_client_sync_async_parity() -> None:
    sync_names = _public_names(RtDbHttpClient)
    async_names = _public_names(RtDbAsyncHttpClient) - _ASYNC_ONLY
    assert sync_names == async_names
