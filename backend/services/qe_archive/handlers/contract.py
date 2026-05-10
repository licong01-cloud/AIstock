"""Handler interface contract for paper_v2 / factor_value archive events.

Authoritative spec source:
- D5 [DECISION] drawer 9cd6d6bb (Codex reply Q1-Q4 + T8-A) — interface boundary
- paper-v2 T13 — `routing_class` discriminator on outbox payload
- docs/architecture/data_warehouse_extension_design_20260510.md §3 routing + §7
  factor_value layout

Design points encoded here (do NOT relax without re-running D5 review):

- **routing_class='archive'**: handlers ONLY consume events whose payload
  carries `routing_class == 'archive'`. paper.daemon.* telemetry events use
  `routing_class='telemetry'` (paper-v2 T13) and must NOT be archived. The
  default `can_handle` enforces this gate so subclasses cannot bypass it
  silently.

- **payload = schema_version + minimal identifier (Q2.b)**: payload is a thin
  envelope; handlers MUST fetch authoritative rows by primary key from the
  source-of-truth table (paper_v2 PG schema, single/{name}.parquet, etc.).
  Payloads are never the source of truth, so replays remain consistent even
  after upstream rows are corrected.

- **schema_version**: required on every payload. Handlers MUST refuse unknown
  versions with `PayloadValidationError` instead of best-effort parsing — D5
  Q2.b mandates fail-fast on contract drift. No silent fallback.

- **per-handler batch_size + coalescing knob (Q2.c)**: handlers expose
  `batch_size` (max authoritative rows fetched per `handle()` call) and
  `coalesce_window_seconds` (sibling events merged into one fetch). Worker
  reuses existing retry/timeout/dead-letter config — no new knobs there.

- **No silent errors**: per project memory feedback_no_silent_errors.md and
  feedback_per_factor_immediate_insert.md, handlers raise on validation /
  fetch / write failure. The worker translates exceptions into
  fail_archive_job + fail_outbox_event with retry; handlers themselves do not
  swallow.
"""

from __future__ import annotations

import abc
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, ClassVar

from ..models import ArchiveJobRecord, ClaimedOutboxEvent

ROUTING_CLASS_ARCHIVE = "archive"
PAYLOAD_SCHEMA_VERSION_KEY = "schema_version"
PAYLOAD_ROUTING_CLASS_KEY = "routing_class"


class HandlerStatus(str, Enum):
    """Terminal status for a single handler invocation.

    SUCCESS — rows landed (insert or upsert), event can be marked complete.
    NOOP — coalesced / already-archived / no authoritative rows; event still
        completes, but the worker stats record zero work.
    FAILED — handler refuses to complete; worker fails the job and retries
        per existing outbox retry config.
    """

    SUCCESS = "success"
    NOOP = "noop"
    FAILED = "failed"


@dataclass(frozen=True)
class ArchiveResult:
    """Outcome of one `handle()` call.

    Per D5 Q3.b interface contract. `rows_inserted` and `rows_upserted` are
    reported separately so the worker / dashboards can distinguish first-time
    captures from idempotent replays (Q3.b idempotency requirement).
    """

    status: HandlerStatus
    rows_inserted: int = 0
    rows_upserted: int = 0
    error_message: str | None = None
    stats: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.rows_inserted < 0 or self.rows_upserted < 0:
            raise ValueError("row counts must be non-negative")
        if self.status is HandlerStatus.FAILED and not self.error_message:
            raise ValueError("FAILED status requires error_message")
        if self.status is not HandlerStatus.FAILED and self.error_message:
            raise ValueError("error_message only valid with FAILED status")


class PayloadValidationError(ValueError):
    """Raised when payload is missing required envelope fields or carries an
    unsupported schema_version. Per D5 Q2.b: fail-fast on contract drift,
    never best-effort parse."""


class UnsupportedEventError(ValueError):
    """Raised by the worker dispatcher when no handler accepts an event.

    Handlers themselves should return `False` from `can_handle` rather than
    raising; this exception is reserved for the dispatch layer.
    """


class ArchiveHandler(abc.ABC):
    """Abstract base for paper_v2 / factor_value archive handlers.

    Subclasses MUST set:
      - `event_type`: outbox event_type string this handler claims
        (e.g. 'paper.portfolio_run.completed', 'factor.recompute.completed')
      - `supported_schema_versions`: tuple of accepted payload schema_version
        strings. Anything else raises PayloadValidationError.

    Subclasses MAY override:
      - `batch_size` (default 100): max authoritative rows fetched per call.
      - `coalesce_window_seconds` (default 0 = no coalescing): worker-side
        knob; handler honors whatever pre-coalesced batch the worker delivers.

    `can_handle` is intentionally final-by-convention: it enforces both the
    event_type match and `routing_class='archive'` gate (Q2.a + paper-v2 T13).
    Subclasses extend acceptance via narrower validation inside `handle()`,
    not by relaxing the gate.
    """

    event_type: ClassVar[str] = ""
    supported_schema_versions: ClassVar[tuple[str, ...]] = ()
    batch_size: ClassVar[int] = 100
    coalesce_window_seconds: ClassVar[int] = 0

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if cls.__abstractmethods__:
            return
        if not cls.event_type:
            raise TypeError(
                f"{cls.__name__} must declare a non-empty class-level `event_type`"
            )
        if not cls.supported_schema_versions:
            raise TypeError(
                f"{cls.__name__} must declare at least one entry in "
                "`supported_schema_versions`"
            )
        if cls.batch_size <= 0:
            raise TypeError(f"{cls.__name__}.batch_size must be > 0")
        if cls.coalesce_window_seconds < 0:
            raise TypeError(
                f"{cls.__name__}.coalesce_window_seconds must be >= 0"
            )

    def can_handle(self, event: ClaimedOutboxEvent) -> bool:
        """Return True iff this handler should process `event`.

        Gate (Q2.a + paper-v2 T13):
          1. event.event_type matches the subclass declaration
          2. payload['routing_class'] == 'archive'

        Telemetry events (paper.daemon.* with routing_class='telemetry') are
        rejected here so they never enter the archive pipeline.
        """

        if event.event_type != self.event_type:
            return False
        payload = event.payload or {}
        return payload.get(PAYLOAD_ROUTING_CLASS_KEY) == ROUTING_CLASS_ARCHIVE

    def validate_payload(self, payload: Mapping[str, Any]) -> None:
        """Enforce envelope contract. Raises PayloadValidationError on drift.

        Subclasses override to add identifier-presence checks but should call
        `super().validate_payload(payload)` first so envelope rules stay in
        one place.
        """

        if not isinstance(payload, Mapping):
            raise PayloadValidationError(
                f"payload must be a mapping, got {type(payload).__name__}"
            )
        version = payload.get(PAYLOAD_SCHEMA_VERSION_KEY)
        if not version:
            raise PayloadValidationError(
                f"payload missing required '{PAYLOAD_SCHEMA_VERSION_KEY}'"
            )
        if version not in self.supported_schema_versions:
            raise PayloadValidationError(
                f"unsupported schema_version={version!r} for "
                f"{type(self).__name__}; supported="
                f"{self.supported_schema_versions}"
            )
        routing = payload.get(PAYLOAD_ROUTING_CLASS_KEY)
        if routing != ROUTING_CLASS_ARCHIVE:
            raise PayloadValidationError(
                f"payload routing_class={routing!r} is not 'archive'; "
                "this handler must not be invoked on non-archive events"
            )

    @abc.abstractmethod
    def handle(
        self,
        event: ClaimedOutboxEvent,
        archive_job: ArchiveJobRecord,
    ) -> ArchiveResult:
        """Fetch authoritative rows by id from the payload and UPSERT.

        Implementations MUST:
          1. Call `self.validate_payload(event.payload)` first.
          2. Resolve the minimal identifier(s) from the payload to
             authoritative source rows (Q2.b — payload is NOT source of truth).
          3. UPSERT into the matching qe_archive table by natural key,
             producing replay-idempotent writes (Q3.b).
          4. Return an ArchiveResult; raise on unrecoverable error so the
             worker fails the job (no silent swallow).

        `archive_job` is provided so handlers may attach incremental stats
        (rows scanned, batches issued) via repository APIs if needed.
        """
