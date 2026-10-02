"""Storage-stub mixin for the in-memory harness: blob upload/delete,
metadata reads, and the synthetic URL handles. Extracted from
``store.py`` (QA-007); methods move verbatim onto
``_InMemoryStoreCore`` via mixin assembly in ``__init__.py``."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from ..errors import ErrorCode, RtDbError
from .store import (
    FileMetadata as FileMetadata,
)
from .store import (
    StoredBlob as StoredBlob,
)
from .store import (
    UploadResult as UploadResult,
)
from .store import (
    _base36 as _base36,
)
from .store import (
    _sha256_hex as _sha256_hex,
)

if TYPE_CHECKING:
    from .store import _InMemoryStoreCore as _Core
else:
    _Core = object


class _StorageEngine(_Core):
    """_StorageEngine: methods extracted verbatim from ``_InMemoryStoreCore``."""

    def upload(self, data: bytes, content_type: str | None = None) -> UploadResult:
        """Store ``data`` and return a server-shaped :class:`UploadResult`. The
        id is a short counter-prefixed token (distinct in shape from document ids)."""
        self._id_counter += 1
        new_id = f"f{_base36(self._id_counter)}"
        digest = _sha256_hex(data)
        size = len(data)
        created_at = self._now()
        self._storage[new_id] = StoredBlob(
            bytes=data,
            content_type=content_type,
            created_at=created_at,
            sha256=digest,
        )
        return UploadResult(id=new_id, sha256=digest, size=size, content_type=content_type)

    def delete_file(self, id: str) -> None:
        """Delete a stored blob. ``NOT_FOUND`` if unknown."""
        if self._storage.pop(id, None) is None:
            raise RtDbError(ErrorCode.NOT_FOUND, "unknown file")

    def get_file_metadata(self, id: str) -> FileMetadata:
        """Read back a stored blob's metadata (``sha256`` is empty — only the
        upload result carries the real digest). ``NOT_FOUND`` if unknown."""
        blob = self._storage.get(id)
        if blob is None:
            raise RtDbError(ErrorCode.NOT_FOUND, "unknown file")
        return FileMetadata(
            id=id,
            sha256="",
            size=len(blob.bytes),
            content_type=blob.content_type,
            creation_time=blob.created_at,
        )

    def get_url(self, id: str) -> str:
        """Synthetic handle — no real byte stream."""
        return f"memory://{id}"

    def transform_url(
        self,
        id: str,
        *,
        w: int | None = None,
        h: int | None = None,
        fit: Literal["cover", "contain", "scale-down"] | None = None,
        q: int | None = None,
        format: Literal["jpeg", "png", "auto"] | None = None,
    ) -> str:
        """Synthetic handle with image-transform params (ENH-014). No real byte stream.

        Params appear in the deterministic order ``w, h, fit, q, format``; unset
        params (and ``format="auto"``, the server default) are omitted. Mirrors
        ``RtDbHttpClient.transform_url`` so tests against the in-memory harness
        assert the same query-string shape.
        """
        parts: list[str] = []
        if w is not None:
            parts.append(f"w={w}")
        if h is not None:
            parts.append(f"h={h}")
        if fit is not None:
            parts.append(f"fit={fit}")
        if q is not None:
            parts.append(f"q={q}")
        # "auto" is the server default — omit so the URL stays minimal (rust parity).
        if format is not None and format != "auto":
            parts.append(f"format={format}")
        base = f"memory://{id}"
        return f"{base}?{'&'.join(parts)}" if parts else base
