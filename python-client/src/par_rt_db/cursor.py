"""Opaque keyset cursor codec: base64 of a JSON array (parity with TS/Rust clients)."""

from __future__ import annotations

import base64
import json
from typing import Any


def encode_cursor(values: list[Any]) -> str:
    """Encode a sort-tuple into an opaque base64 cursor string."""
    raw = json.dumps(values, separators=(",", ":")).encode("utf-8")
    return base64.b64encode(raw).decode("ascii")


def decode_cursor(cursor: str) -> list[Any]:
    """Decode an opaque cursor back into the sort-tuple.

    Args:
        cursor: The opaque base64 string produced by :func:`encode_cursor`.

    Returns:
        The decoded JSON array.

    Raises:
        ValueError: If the input is not valid base64, not valid JSON,
            or does not decode to a JSON array.
    """
    try:
        raw = base64.b64decode(cursor.encode("ascii"), validate=True)
        values = json.loads(raw)
    except (ValueError, json.JSONDecodeError) as err:
        raise ValueError("invalid cursor") from err
    if not isinstance(values, list):
        raise ValueError("cursor must decode to a JSON array")
    return values
