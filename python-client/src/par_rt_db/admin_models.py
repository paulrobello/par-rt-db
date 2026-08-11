"""Shared HTTP/admin response models for par-rt-db (ARC-108).

The canonical home of the camelCase-on-the-wire / snake_case-in-Python response
models used by every admin-bearing client (sync/async × data-plane/admin).
Formerly defined in :mod:`par_rt_db.http_client`, these were moved out so the
canonical admin module (:mod:`par_rt_db.admin`) no longer depends on the module
it supersedes (ARC-108 collapsed the four duplicated admin model definitions into
this one home).

:mod:`par_rt_db.http_client` and :mod:`par_rt_db.aio_http_client` re-import
these names so every existing ``from par_rt_db.http_client import X`` continues
to resolve to the same class object (the model-identity regression at
``test_http_client.py::test_top_level_minted_token_is_http_client_minted_token``
locks this down). There is exactly one model type per response shape across the
sync/async data-plane and admin clients.

The storage models (``UploadResult`` / ``FileMetadata`` / ``SignedUrl``) are
data-plane shapes and stay defined in :mod:`par_rt_db.http_client`; they reuse
the :class:`_Wire` base re-exported from here.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from .mutation import StepResult
from .schema import SchemaDef
from .wire import to_camel


class _Wire(BaseModel):
    """camelCase wire keys, reject unknown fields — for HTTP response models."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        alias_generator=to_camel,
    )


class MintedToken(_Wire):
    """``POST /admin/mint-token`` response: ``{tokenId, token}``."""

    token_id: str
    token: str


class AdminMember(_Wire):
    """``GET /admin/admins`` row — ``{email, githubId?}``."""

    email: str
    github_id: int | None = None


class TokenInfo(_Wire):
    """``GET /admin/tokens`` row — ``{id, name, createdAt, revoked, expiresAt,
    readOnly, tables}``.

    The last three fields default so an older server that omits them still
    deserializes (matches the server's ``#[serde(default)]`` on
    ``TokenRow``); a current server always sends them. ``expiresAt``/``tables``
    are ``None`` for a full-access token; ``readOnly`` is ``False`` for a
    read-write token.
    """

    id: str
    name: str
    created_at: int
    revoked: bool
    expires_at: int | None = None
    read_only: bool = False
    tables: list[str] | None = None


class SessionInfo(_Wire):
    """One active interactive session row from ``GET /admin/sessions``.

    ``tokenHash`` is a non-reversible sha256 digest (the plaintext token is
    never stored), safe to surface to an admin and used to target a row for
    revoke. ``email``/``login`` are ``None`` when the user has none (e.g. an
    anonymous session). Mirrors the ts-client ``SessionInfo`` byte-for-byte
    (camelCase on the wire).
    """

    token_hash: str
    user_id: str
    email: str | None = None
    login: str | None = None
    anonymous: bool = False
    created_at: int
    expires_at: int


class TableStat(_Wire):
    """One row of ``DbStats.tables`` — ``{name, rowCount, sizeBytes}``."""

    name: str
    row_count: int
    size_bytes: int


class DbStats(_Wire):
    """``GET /admin/dbs/{db}/stats`` response — per-table row counts + sizes
    plus the ENH-011 per-db quota/usage triple (``0`` = unlimited). The server
    always emits all eight fields; ``extra='forbid'`` means a response missing
    any would be rejected, matching the server's non-optional ``DbStatsResponse``.
    """

    tables: list[TableStat]
    total_size_bytes: int
    tables_quota: int
    tables_used: int
    storage_quota_bytes: int
    storage_used_bytes: int
    subs_quota: int
    subs_used: int


class LatencyStats(_Wire):
    """p50/p95/p99 latency percentile triple (microseconds). Field names are
    already lowercase, so ``to_camel`` leaves them as ``p50``/``p95``/``p99``."""

    p50: int
    p95: int
    p99: int


class DbSubCounters(_Wire):
    """Per-db subscription counter row — the shape shared by ``perDb[]`` on
    ``GET /admin/subscriptions`` and ``perDbSubs[]`` on ``GET /admin/metrics``.
    Mirrors ``server::subs::DbSubCounters``."""

    db: str
    reruns: int
    skips_point: int
    skips_indexed: int
    skips_ordered: int
    missed: int


class MetricsSnapshot(_Wire):
    """``GET /admin/metrics`` response — server-wide counters and gauges.

    The ``subs_*`` fields default to 0 so a client built against a newer server
    still deserializes an older server's response (these counters landed
    2026-07-29); 0 is the correct "not reported" value for a monotonic counter.
    ``per_db_subs`` defaults to empty for the same reason (added 2026-08-05 with
    the live subscription inspector, ENH-010).
    """

    queries_total: int
    mutations_total: int
    uploads_total: int
    ws_connections: int
    active_subscriptions: int
    pool_size: int
    pool_idle: int
    uptime_seconds: int
    query_latency: LatencyStats
    mutate_latency: LatencyStats
    subscribe_latency: LatencyStats
    subs_reruns_total: int = 0
    subs_skips_point_total: int = 0
    subs_skips_indexed_total: int = 0
    subs_skips_ordered_total: int = 0
    subs_skip_verifications_total: int = 0
    subs_missed_pushes_total: int = 0
    per_db_subs: list[DbSubCounters] = Field(default_factory=list)


class HotConfig(_Wire):
    """Runtime-mutable hot config — mirrors ``server::config::HotConfig``."""

    allowed_origins: list[str]
    session_ttl_days: int
    max_file_size: int
    idempotency_ttl_ms: int
    # Per-db resource quotas (ENH-011); 0 = unlimited. Mirrors server HotConfig.
    max_tables_per_db: int = 0
    max_storage_bytes_per_db: int = 0
    max_subs_per_db: int = 0


class ConfigResponse(_Wire):
    """``GET /admin/config`` response — redacted boot config + hot config + build
    identity + admin allowlist. Secrets surface as configured-bools, never values."""

    port: int
    public_url: str
    github_base_url: str
    github_api_url: str
    database_url_configured: bool
    admin_key_configured: bool
    github_configured: bool
    google_configured: bool
    gitlab_configured: bool
    oidc_configured: bool
    hot: HotConfig
    version: str
    git_commit: str
    admins: list[AdminMember]


class HotConfigPatch(_Wire):
    """``PATCH /admin/config`` body — every field optional; omitted fields are
    left unchanged. Serialize with ``exclude_none=True`` so the wire body carries
    only the fields the caller sets (matches rust-client's
    ``skip_serializing_if = "Option::is_none"``). Unknown fields are rejected
    server-side (``deny_unknown_fields``).
    """

    allowed_origins: list[str] | None = None
    session_ttl_days: int | None = None
    max_file_size: int | None = None
    idempotency_ttl_ms: int | None = None
    max_tables_per_db: int | None = None
    max_storage_bytes_per_db: int | None = None
    max_subs_per_db: int | None = None


class OpEvent(_Wire):
    """``GET /admin/ops/recent`` row — one document-op event from the in-memory
    ring. ``kind`` is a lowercase string (``insert``/``patch``/``replace``/
    ``delete``/``upsert``); ``owner`` is ``null`` for admin/machine writes."""

    db: str
    table: str
    doc_id: str
    kind: str
    ts: int
    owner: str | None = None


class CastFailure(_Wire):
    """One row of ``DirectiveReport.cast_failures`` — a value that could not be
    coerced under ``changeType.cast``. Mirrors ``server::migrate::CastFailure``."""

    id: str
    value: Any


class SampleChange(_Wire):
    """One row of ``DirectiveReport.sample_changes`` — a before/after pair for a
    row touched by a migration directive. Mirrors ``server::migrate::SampleChange``."""

    id: str
    before: Any
    after: Any


class DirectiveReport(_Wire):
    """Per-directive outcome. ``castFailures`` and ``sampleChanges`` are
    ``skip_serializing_if = "Vec::is_empty"`` on the server, so they surface as
    optional on the wire (absent when empty). Mirrors
    ``server::migrate::DirectiveReport``."""

    op: str
    affected_rows: int
    cast_failures: list[CastFailure] = Field(default_factory=list)
    sample_changes: list[SampleChange] = Field(default_factory=list)


class MigrateResult(_Wire):
    """``POST /admin/db/{db}/migrate`` response. ``schema`` is the post-migration
    derived schema — returned even on ``dryRun`` (with ``applied: false``), so a
    caller can preview the resulting shape. Mirrors ``server::migrate::MigrateResult``.

    The Python attribute is ``schema_`` (trailing underscore) because pydantic v2's
    ``BaseModel`` still carries the deprecated v1 ``.schema()`` method, and a field
    literally named ``schema`` shadows it (pydantic emits a ``UserWarning``). The
    wire alias is the server-contract ``"schema"``.
    """

    applied: bool
    schema_: SchemaDef = Field(alias="schema")
    directives: list[DirectiveReport]


class SchemaHistorySummary(_Wire):
    """One row of ``GET /admin/db/{db}/schema/history`` (newest-first). Mirrors
    ``server::schema_history::HistorySummary``. ``source`` is the event that
    captured the snapshot: ``"push"`` | ``"migrate"`` | ``"restore"``."""

    version: int
    captured_at: int
    source: str
    principal: str | None = None


class SchemaHistoryEntry(SchemaHistorySummary):
    """One full snapshot from ``GET /admin/db/{db}/schema/history/{version}``,
    adding the ``schema`` blob. Mirrors ``server::schema_history::HistoryEntry``.

    ``schema_`` is the raw captured JSON (a serialized ``SchemaDef``), kept as a
    plain dict so an older snapshot never fails to validate. The trailing
    underscore mirrors :class:`MigrateResult` (pydantic v2 reserves ``.schema()``).
    """

    schema_: dict[str, Any] = Field(alias="schema")


class Webhook(_Wire):
    """``GET /admin/db/{db}/webhooks`` row — one registered webhook.

    Mirrors ``server::webhook::Webhook``. ``table`` is ``None`` for an
    all-tables webhook (the server's ``tbl = None``); ``events`` is the
    matched op-name list (``["*"]`` for all events). ``created_at`` is
    epoch-millis. ``secret`` is the per-webhook HMAC signing key (SEC-115);
    the receiver uses it to verify each delivery's ``X-Rtdb-Signature``
    header. Server-generated; surfaced here so an operator can copy it to the
    receiver. ``None`` only before the boot backfill has run (or against an
    older server that omits the field).
    """

    id: int
    db: str
    table: str | None = None
    url: str
    events: list[str]
    created_at: int
    enabled: bool
    secret: str | None = None


class WebhookDelivery(_Wire):
    """``GET /admin/db/{db}/webhooks/{id}/deliveries`` row — one delivery from
    the outbox.

    Mirrors ``server::webhook::DeliveryRow``. ``status`` is ``pending`` /
    ``retrying`` / ``delivered`` / ``failed``. ``next_attempt`` is epoch-millis
    (the due time of the next retry, monotonic under backoff). ``last_error``
    is ``None`` once a delivery succeeds (or before any attempt). ``payload`` is
    the opaque queued JSONB body — passed through verbatim so callers can
    inspect the exact event the worker will/did POST.
    """

    id: int
    attempts: int
    status: str
    next_attempt: int
    last_error: str | None = None
    payload: Any


class AuditEntry(_Wire):
    """``GET /admin/audit`` row — one durable audit-log record.

    Mirrors ``server::audit::AuditEntry``. ``op`` is ``None`` for rows where the
    op kind was not recorded; ``principal`` is ``None`` for system-initiated
    writes (scheduled jobs, TTL reaper — ``owner = None`` at the tap site) where
    no interactive user was involved. ``ts_ms`` is epoch-millis.
    """

    id: int
    ts_ms: int
    db: str
    table: str
    op: str | None = None
    doc_id: str
    principal: str | None = None
    source: str


class SubscriptionsPrincipal(_Wire):
    """Interactive principal on a live subscription row — ``null`` for machine
    tokens, scheduled jobs, and admin bypass (no interactive identity)."""

    user_id: str | None
    email: str | None


class SubscriptionInfo(_Wire):
    """``GET /admin/subscriptions`` row — one live subscription.

    ``read_set_class`` is one of ``point``/``indexed``/``ordered``/``table``
    (the four :class:`subs::ReadSet` variants); ``terminal`` is the query's
    terminal operation (``get``/``count``/``collect``/``unique``/…)."""

    db: str
    table: str
    terminal: str
    read_set_class: str
    principal: SubscriptionsPrincipal | None


class SubscriptionsResponse(_Wire):
    """``GET /admin/subscriptions`` response — live subscription rows plus the
    subscription fan-out counters (rerun/skip totals, per-db breakdown)."""

    subscriptions: list[SubscriptionInfo]
    subs_reruns_total: int
    subs_skips_point_total: int
    subs_skips_indexed_total: int
    subs_skips_ordered_total: int
    subs_missed_pushes_total: int
    per_db: list[DbSubCounters]


# ``StepResult`` is a ``Union`` alias (no ``model_validate``); route through a
# single ``TypeAdapter`` for the untagged per-step result, mirroring mutation.py
# and the data-plane HTTP clients.
_STEP_RESULT_ADAPTER = TypeAdapter(StepResult)


# Sentinel for ``edit_webhook``'s ``table`` kwarg that distinguishes "caller did
# not pass this kwarg" (``_UNSET`` → omit from the body → server leaves the field
# unchanged) from "caller passed ``None``" (``table=None`` → send JSON ``null``
# → server clears the field). Only ``edit_webhook`` needs the tri-state —
# ``create_webhook`` treats ``table=None`` as all-tables (matching the server's
# create semantics), so it does not use the sentinel.
_UNSET: Any = object()
