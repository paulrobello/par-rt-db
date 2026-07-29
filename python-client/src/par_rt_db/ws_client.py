"""Reactive WebSocket client for par-rt-db (the ``[ws]`` extra).

Async client over the ``/sync`` endpoint: one multiplexed connection, live
subscriptions (``async for value in client.subscribe(query)``), at-most-once
mutations, and schedule ops. Mirrors ``ts-client/src/client.ts`` and
``rust-client/src/ws.rs``. ``websockets`` is imported lazily inside the default
``connect`` factory so this module imports without the ``[ws]`` extra installed.
"""

from __future__ import annotations

import json
from typing import Any


def _sync_url(url: str) -> str:
    """Flip http(s)→ws(s), strip trailing slashes, append ``/sync``."""
    u = url.strip()
    if u.startswith("https://"):
        u = "wss://" + u[len("https://") :]
    elif u.startswith("http://"):
        u = "ws://" + u[len("http://") :]
    return u.rstrip("/") + "/sync"


def _canonical_key(query_dict: dict[str, Any]) -> str:
    """Stable dedup key for a query's wire dict (order-independent)."""
    return json.dumps(query_dict, sort_keys=True, separators=(",", ":"))


def _backoff_delay(attempt: int, base: float, max_delay: float, rand: float) -> float:
    """Jittered exponential backoff: ``min(max, base * 2**attempt) * (0.5 + rand*0.5)``."""
    raw = min(max_delay, base * (2**attempt))
    return raw * (0.5 + rand * 0.5)
