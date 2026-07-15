"""Append-only persistence for Phase 1 Selection stage-trace envelopes.

The outbox is intentionally separate from Selection/Advisory business tables.
An exact retry returns the stored immutable envelope; a different payload for
the same Selection identity fails closed.  Delivery events form one append-only
chain so future observation writers can retry without rerunning Selection.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from enum import Enum
from queue import Full, Queue
from threading import Thread
from typing import Any, Callable, Iterator, Literal, Mapping, Protocol, Sequence

import psycopg2
import psycopg2.extras
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase1.source_ledger import SourceLedgerError
from backend.services.advisory_phase1.stage_trace import (
    StageTraceEnvelope,
    TraceCaptureBinding,
)


REASON_TRACE_OUTBOX_CONFLICT = "ADVISORY_PHASE1_TRACE_OUTBOX_CONFLICT"
REASON_TRACE_OUTBOX_CHAIN_INVALID = "ADVISORY_PHASE1_TRACE_OUTBOX_CHAIN_INVALID"
REASON_TRACE_OUTBOX_RECORD_INVALID = "ADVISORY_PHASE1_TRACE_OUTBOX_RECORD_INVALID"
REASON_TRACE_ADMISSION_UNAVAILABLE = "ADVISORY_PHASE1_TRACE_ADMISSION_UNAVAILABLE"
REASON_TRACE_DISPATCH_QUEUE_FULL = "ADVISORY_PHASE1_TRACE_DISPATCH_QUEUE_FULL"
REASON_TRACE_DISPATCHER_FAILED = "ADVISORY_PHASE1_TRACE_DISPATCHER_FAILED"
REASON_TRACE_CAPTURE_LOST = "ADVISORY_PHASE1_TRACE_CAPTURE_LOST"
REASON_TRACE_WRITE_FAILED = "ADVISORY_PHASE1_TRACE_WRITE_FAILED"
REASON_PHASE1F2_TRACE_IDENTITY_INVALID = "ADVISORY_PHASE1F2_TRACE_IDENTITY_INVALID"
REASON_PHASE1F2_SCHEMA_NOT_READY = "ADVISORY_PHASE1F2_SCHEMA_NOT_READY"
REASON_PHASE1F2_OUTBOX_SCOPE_CONFLICT = "ADVISORY_PHASE1F2_OUTBOX_SCOPE_CONFLICT"
TRACE_IDENTITY_SCHEMA_VERSION_V2 = "advisory_phase1_trace_identity_v2"


class TraceDeliveryEventType(str, Enum):
    OBSERVATION_WRITTEN = "OBSERVATION_WRITTEN"
    OBSERVATION_WRITE_FAILED = "OBSERVATION_WRITE_FAILED"


class TraceDeliveryEventRequest(BaseModel):
    """Writer result without caller-controlled event time or chain identity."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    trace_outbox_id: str = Field(min_length=1, max_length=160)
    delivery_event_no: int = Field(ge=1)
    event_type: TraceDeliveryEventType
    predecessor_event_hash: str | None = Field(
        default=None, min_length=64, max_length=64
    )
    writer_attempt_no: int = Field(ge=1)
    reason_codes: tuple[str, ...] = ()
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("predecessor_event_hash")
    @classmethod
    def _sha(cls, value: str | None) -> str | None:
        if value is not None and (
            len(value) != 64 or any(char not in "0123456789abcdef" for char in value)
        ):
            raise ValueError("predecessor_event_hash must be lowercase sha256 hex")
        return value

    @model_validator(mode="after")
    def _sequence_shape(self) -> "TraceDeliveryEventRequest":
        if self.delivery_event_no == 1 and self.predecessor_event_hash is not None:
            raise ValueError("first delivery event cannot have a predecessor")
        if self.delivery_event_no > 1 and self.predecessor_event_hash is None:
            raise ValueError("non-first delivery event requires predecessor_event_hash")
        return self

    def request_payload(self) -> dict[str, Any]:
        return {
            "trace_outbox_id": self.trace_outbox_id,
            "delivery_event_no": self.delivery_event_no,
            "event_type": self.event_type.value,
            "predecessor_event_hash": self.predecessor_event_hash,
            "writer_attempt_no": self.writer_attempt_no,
            "reason_codes": list(self.reason_codes),
            "payload": _canonicalize(self.payload),
        }

    @property
    def request_hash(self) -> str:
        return _canonical_json_sha256(self.request_payload())


class TraceDeliveryEvent(BaseModel):
    """Persisted immutable delivery event, materialized with repository DB time."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    delivery_event_id: str
    delivery_event_hash: str
    request: TraceDeliveryEventRequest
    event_at: datetime

    @field_validator("event_at")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("event_at must be timezone-aware")
        return value.astimezone(timezone.utc)

    @classmethod
    def from_request(
        cls, request: TraceDeliveryEventRequest, *, event_at: datetime
    ) -> "TraceDeliveryEvent":
        payload = {
            **request.request_payload(),
            "event_at": event_at.astimezone(timezone.utc),
        }
        event_hash = _canonical_json_sha256(payload)
        return cls(
            delivery_event_id=f"std_{event_hash[:20]}",
            delivery_event_hash=event_hash,
            request=request,
            event_at=event_at,
        )


class TraceOutboxRecord(BaseModel):
    """The exact envelope and binding stored by the append-only outbox."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    trace_outbox_id: str
    envelope: StageTraceEnvelope
    binding: TraceCaptureBinding
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def _identity_matches(self) -> "TraceOutboxRecord":
        if self.trace_outbox_id != self.envelope.trace_outbox_id:
            raise ValueError("outbox record identity does not match envelope")
        binding = self.envelope.trace_content.get("trace_capture_binding")
        if _canonicalize(binding) != _canonicalize(
            self.binding.model_dump(mode="json")
        ):
            raise ValueError("outbox binding does not match immutable trace content")
        return self


ConnFactory = Callable[[], Iterator[Any]]


class TraceAdmissionValidator(Protocol):
    """Validate and lock persisted admission state in the writer transaction."""

    def validate(
        self,
        *,
        envelope: StageTraceEnvelope,
        binding: TraceCaptureBinding,
        conn: Any | None = None,
    ) -> None: ...


class RejectingTraceAdmissionValidator:
    """Fail closed until the capture-batch state machine provides a real validator."""

    def validate(
        self,
        *,
        envelope: StageTraceEnvelope,
        binding: TraceCaptureBinding,
        conn: Any | None = None,
    ) -> None:
        raise SourceLedgerError(
            REASON_TRACE_ADMISSION_UNAVAILABLE,
            "trace outbox requires persisted control binding and RUNNING capture-batch validation",
            context={
                "trace_outbox_id": envelope.trace_outbox_id,
                "capture_batch_id": binding.capture_batch_id,
            },
        )


class TraceDispatchFailureHandler(Protocol):
    """Durably record an asynchronous write failure without rerunning Selection."""

    def __call__(
        self,
        *,
        envelope: StageTraceEnvelope,
        binding: TraceCaptureBinding,
        reason_code: str,
        error: Exception,
    ) -> None: ...


class BoundedTraceOutboxDispatcher:
    """Bounded daemon dispatcher that keeps PostgreSQL I/O off Selection threads."""

    non_blocking = True

    def __init__(
        self,
        *,
        writer: Any,
        failure_handler: TraceDispatchFailureHandler,
        max_pending: int = 64,
    ) -> None:
        if max_pending < 1:
            raise ValueError("max_pending must be positive")
        self._writer = writer
        self._failure_handler = failure_handler
        self._fatal_error: Exception | None = None
        self._queue: Queue[tuple[StageTraceEnvelope, TraceCaptureBinding] | None] = (
            Queue(maxsize=max_pending)
        )
        self._thread = Thread(
            target=self._run, name="advisory-trace-outbox", daemon=True
        )
        self._thread.start()

    def append(
        self, envelope: StageTraceEnvelope, *, binding: TraceCaptureBinding
    ) -> None:
        if self._fatal_error is not None:
            raise SourceLedgerError(
                REASON_TRACE_DISPATCHER_FAILED,
                "trace outbox dispatcher failure handler is unavailable",
                context={"trace_outbox_id": envelope.trace_outbox_id},
            ) from self._fatal_error
        try:
            self._queue.put_nowait((envelope, binding))
        except Full as exc:
            raise SourceLedgerError(
                REASON_TRACE_DISPATCH_QUEUE_FULL,
                "trace outbox dispatch queue is full",
                context={"trace_outbox_id": envelope.trace_outbox_id},
            ) from exc

    def join(self) -> None:
        self._queue.join()

    def shutdown(self) -> None:
        self.join()
        self._queue.put(None)
        self._thread.join()

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if item is None:
                    return
                envelope, binding = item
                try:
                    self._writer.append(envelope, binding=binding)
                except Exception as exc:
                    try:
                        self._failure_handler(
                            envelope=envelope,
                            binding=binding,
                            reason_code=REASON_TRACE_WRITE_FAILED,
                            error=exc,
                        )
                    except Exception as handler_exc:
                        self._fatal_error = handler_exc
            finally:
                self._queue.task_done()


def _transactional_conn_factory() -> Iterator[Any]:
    from backend.db.pg_pool import get_conn

    return get_conn(autocommit=False, manage_transaction=True)


class InMemoryTraceOutboxRepository:
    """Deterministic outbox oracle for contract tests and local capture wiring."""

    def __init__(
        self,
        *,
        now_provider: Callable[[], datetime] | None = None,
        admission_validator: TraceAdmissionValidator | None = None,
    ) -> None:
        self._records_by_hash: dict[str, TraceOutboxRecord] = {}
        self._records_by_natural_key: dict[
            tuple[str, str, str, str, str, str], TraceOutboxRecord
        ] = {}
        self._events_by_outbox: dict[str, list[TraceDeliveryEvent]] = {}
        self._now_provider = now_provider or (lambda: datetime.now(timezone.utc))
        self._admission_validator = (
            admission_validator or RejectingTraceAdmissionValidator()
        )

    def append(
        self, envelope: StageTraceEnvelope, *, binding: TraceCaptureBinding
    ) -> TraceOutboxRecord:
        _validate_envelope_binding(envelope=envelope, binding=binding)
        existing = self._records_by_hash.get(envelope.trace_content_hash)
        if existing is not None:
            if existing.envelope == envelope and existing.binding == binding:
                return existing
            raise SourceLedgerError(
                REASON_TRACE_OUTBOX_CONFLICT,
                "same trace hash has conflicting immutable payload",
            )
        natural_key = _natural_key(envelope, binding)
        natural_existing = self._records_by_natural_key.get(natural_key)
        if natural_existing is not None:
            raise SourceLedgerError(
                REASON_PHASE1F2_OUTBOX_SCOPE_CONFLICT,
                "same selection identity, capture policy, and admission scope has a different trace payload",
                context={"trace_outbox_id": natural_existing.trace_outbox_id},
            )
        self._admission_validator.validate(
            envelope=envelope, binding=binding, conn=None
        )
        record = TraceOutboxRecord(
            trace_outbox_id=envelope.trace_outbox_id,
            envelope=envelope,
            binding=binding,
            created_at=self._now_provider(),
        )
        self._records_by_hash[envelope.trace_content_hash] = record
        self._records_by_natural_key[natural_key] = record
        self._events_by_outbox[record.trace_outbox_id] = []
        return record

    def append_delivery(self, request: TraceDeliveryEventRequest) -> TraceDeliveryEvent:
        if request.trace_outbox_id not in self._events_by_outbox:
            raise SourceLedgerError(
                REASON_TRACE_OUTBOX_RECORD_INVALID, "trace outbox record does not exist"
            )
        chain = self._events_by_outbox[request.trace_outbox_id]
        if request.delivery_event_no <= len(chain):
            existing = chain[request.delivery_event_no - 1]
            if existing.request.request_hash == request.request_hash:
                return existing
            raise SourceLedgerError(
                REASON_TRACE_OUTBOX_CONFLICT,
                "same delivery event sequence has a different request",
            )
        if request.delivery_event_no != len(chain) + 1:
            raise SourceLedgerError(
                REASON_TRACE_OUTBOX_CHAIN_INVALID,
                "delivery event number must be the next sequence number",
            )
        if chain:
            predecessor = chain[-1]
            if predecessor.delivery_event_hash != request.predecessor_event_hash:
                raise SourceLedgerError(
                    REASON_TRACE_OUTBOX_CHAIN_INVALID,
                    "delivery predecessor does not match prior event",
                )
            if (
                predecessor.request.event_type
                is TraceDeliveryEventType.OBSERVATION_WRITTEN
            ):
                raise SourceLedgerError(
                    REASON_TRACE_OUTBOX_CHAIN_INVALID,
                    "written delivery event is terminal",
                )
        event = TraceDeliveryEvent.from_request(request, event_at=self._now_provider())
        chain.append(event)
        return event

    def contains_identity(self, identity: "ExpectedTraceIdentity") -> bool:
        _require_scope_aware_identity(identity)
        return identity.natural_key in self._records_by_natural_key


_TRACE_OUTBOX_COLUMNS = """
trace_outbox_id, control_binding_event_hash, selection_run_id, package_id,
manifest_sha256, decision_as_of_trade_date, handoff_readiness_hash,
admission_scope_id, admission_scope_hash, capture_batch_id, capture_fencing_token,
trace_schema_version, capture_policy_hash, trace_content_jsonb, trace_content_hash,
candidate_count, size_bytes, created_at
"""

_TRACE_DELIVERY_COLUMNS = """
delivery_event_id, trace_outbox_id, delivery_event_no, predecessor_event_hash,
event_type, writer_attempt_no, event_at, reason_codes, payload_jsonb,
delivery_request_hash, delivery_event_hash
"""


class PostgresTraceOutboxRepository:
    """PostgreSQL persistence for exact trace retries and delivery chains."""

    def __init__(
        self,
        conn_factory: ConnFactory | None = None,
        *,
        admission_validator: TraceAdmissionValidator | None = None,
    ) -> None:
        self._conn_factory = conn_factory or _transactional_conn_factory
        self._admission_validator = (
            admission_validator or RejectingTraceAdmissionValidator()
        )

    def append(
        self, envelope: StageTraceEnvelope, *, binding: TraceCaptureBinding
    ) -> TraceOutboxRecord:
        try:
            return self._append_scope_aware(envelope=envelope, binding=binding)
        except SourceLedgerError:
            raise
        except (
            psycopg2.errors.UndefinedColumn,
            psycopg2.errors.UndefinedObject,
            psycopg2.errors.UndefinedTable,
        ) as exc:
            raise SourceLedgerError(
                REASON_PHASE1F2_SCHEMA_NOT_READY,
                "scope-aware trace outbox schema is incomplete",
            ) from exc

    def _append_scope_aware(
        self, *, envelope: StageTraceEnvelope, binding: TraceCaptureBinding
    ) -> TraceOutboxRecord:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                _require_scope_aware_outbox_schema(cur)
                return self.append_in_transaction(
                    cur,
                    envelope=envelope,
                    persisted_binding=binding,
                    current_writer_binding=binding,
                    validate_admission=lambda: self._admission_validator.validate(
                        envelope=envelope, binding=binding, conn=conn
                    ),
                )

    def append_delivery(self, request: TraceDeliveryEventRequest) -> TraceDeliveryEvent:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                return self.append_delivery_in_transaction(cur, request)

    @staticmethod
    def read_exact_by_hash_in_transaction(
        cur: Any, trace_content_hash: str
    ) -> TraceOutboxRecord:
        cur.execute(
            f"""
            SELECT {_TRACE_OUTBOX_COLUMNS}
            FROM app.advisory_selection_stage_trace_outbox
            WHERE trace_content_hash = %s
            FOR KEY SHARE
            """,
            (trace_content_hash,),
        )
        row = cur.fetchone()
        if row is None:
            raise SourceLedgerError(
                REASON_TRACE_OUTBOX_RECORD_INVALID, "trace outbox record does not exist"
            )
        return _record_from_row(dict(row))

    @staticmethod
    def read_exact_by_hash_readonly(
        cur: Any, trace_content_hash: str
    ) -> TraceOutboxRecord:
        cur.execute(
            f"""
            SELECT {_TRACE_OUTBOX_COLUMNS}
            FROM app.advisory_selection_stage_trace_outbox
            WHERE trace_content_hash = %s
            """,
            (trace_content_hash,),
        )
        row = cur.fetchone()
        if row is None:
            raise SourceLedgerError(
                REASON_TRACE_OUTBOX_RECORD_INVALID, "trace outbox record does not exist"
            )
        return _record_from_row(dict(row))

    @staticmethod
    def read_exact_by_natural_key_in_transaction(
        cur: Any, natural_key: tuple[str, str, str, str, str, str]
    ) -> TraceOutboxRecord:
        cur.execute(
            f"""
            SELECT {_TRACE_OUTBOX_COLUMNS}
            FROM app.advisory_selection_stage_trace_outbox
            WHERE selection_run_id = %s AND package_id = %s AND manifest_sha256 = %s
              AND decision_as_of_trade_date = %s AND capture_policy_hash = %s
              AND admission_scope_hash = %s
            FOR KEY SHARE
            """,
            natural_key,
        )
        row = cur.fetchone()
        if row is None:
            raise SourceLedgerError(
                REASON_TRACE_OUTBOX_RECORD_INVALID,
                "trace outbox natural identity does not exist",
            )
        return _record_from_row(dict(row))

    @staticmethod
    def read_exact_by_natural_key_readonly(
        cur: Any, natural_key: tuple[str, str, str, str, str, str]
    ) -> TraceOutboxRecord | None:
        cur.execute(
            f"""
            SELECT {_TRACE_OUTBOX_COLUMNS}
            FROM app.advisory_selection_stage_trace_outbox
            WHERE selection_run_id = %s AND package_id = %s AND manifest_sha256 = %s
              AND decision_as_of_trade_date = %s AND capture_policy_hash = %s
              AND admission_scope_hash = %s
            """,
            natural_key,
        )
        row = cur.fetchone()
        return _record_from_row(dict(row)) if row is not None else None

    @staticmethod
    def append_in_transaction(
        cur: Any,
        *,
        envelope: StageTraceEnvelope,
        persisted_binding: TraceCaptureBinding,
        current_writer_binding: TraceCaptureBinding,
        validate_admission: Callable[[], None] | None = None,
    ) -> TraceOutboxRecord:
        _validate_envelope_binding(envelope=envelope, binding=persisted_binding)
        if _binding_semantic_payload(persisted_binding) != _binding_semantic_payload(
            current_writer_binding
        ):
            raise SourceLedgerError(
                REASON_TRACE_OUTBOX_CONFLICT,
                "persisted and current trace bindings differ semantically",
            )
        natural_key = _natural_key(envelope, persisted_binding)
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))", (envelope.trace_outbox_id,)
        )
        cur.execute(
            f"""
            SELECT {_TRACE_OUTBOX_COLUMNS}
            FROM app.advisory_selection_stage_trace_outbox
            WHERE trace_content_hash = %s
            FOR UPDATE
            """,
            (envelope.trace_content_hash,),
        )
        existing = cur.fetchone()
        if existing is not None:
            record = _record_from_row(dict(existing))
            if record.envelope == envelope and record.binding == persisted_binding:
                return record
            raise SourceLedgerError(
                REASON_TRACE_OUTBOX_CONFLICT,
                "same trace hash has conflicting immutable payload",
            )
        cur.execute(
            f"""
            SELECT {_TRACE_OUTBOX_COLUMNS}
            FROM app.advisory_selection_stage_trace_outbox
            WHERE selection_run_id = %s AND package_id = %s AND manifest_sha256 = %s
              AND decision_as_of_trade_date = %s AND capture_policy_hash = %s
              AND admission_scope_hash = %s
            FOR UPDATE
            """,
            natural_key,
        )
        if cur.fetchone() is not None:
            raise SourceLedgerError(
                REASON_PHASE1F2_OUTBOX_SCOPE_CONFLICT,
                "same selection identity, capture policy, and admission scope has a different trace payload",
            )
        if persisted_binding != current_writer_binding:
            raise SourceLedgerError(
                REASON_TRACE_OUTBOX_CONFLICT,
                "a new outbox row must persist the current writer binding",
            )
        if validate_admission is not None:
            validate_admission()
        cur.execute("SELECT clock_timestamp() AS created_at")
        created_at = cur.fetchone()["created_at"]
        try:
            cur.execute(
                f"""
                INSERT INTO app.advisory_selection_stage_trace_outbox (
                    trace_outbox_id, control_binding_event_hash, selection_run_id, package_id, manifest_sha256,
                    decision_as_of_trade_date, handoff_readiness_hash, admission_scope_id,
                    admission_scope_hash, capture_batch_id, capture_fencing_token,
                    trace_schema_version, capture_policy_hash, trace_content_jsonb,
                    trace_content_hash, candidate_count, size_bytes, created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                ) RETURNING {_TRACE_OUTBOX_COLUMNS}
                """,
                _outbox_insert_params(
                    envelope=envelope, binding=persisted_binding, created_at=created_at
                ),
            )
        except psycopg2.IntegrityError as exc:
            constraint_name = str(getattr(exc.diag, "constraint_name", "") or "")
            if constraint_name != "uq_advisory_stage_trace_outbox_scope_identity":
                raise SourceLedgerError(
                    REASON_TRACE_OUTBOX_CONFLICT,
                    "database rejected trace outbox persistence",
                    context={"constraint_name": constraint_name or None},
                ) from exc
            raise SourceLedgerError(
                REASON_PHASE1F2_OUTBOX_SCOPE_CONFLICT,
                "database rejected scope-aware trace outbox identity",
            ) from exc
        return _record_from_row(dict(cur.fetchone()))

    @staticmethod
    def read_delivery_chain_exact_in_transaction(
        cur: Any, trace_outbox_id: str
    ) -> tuple[TraceDeliveryEvent, ...]:
        cur.execute(
            f"""
            SELECT {_TRACE_DELIVERY_COLUMNS}
            FROM app.advisory_selection_stage_trace_delivery_event
            WHERE trace_outbox_id = %s
            ORDER BY delivery_event_no
            FOR KEY SHARE
            """,
            (trace_outbox_id,),
        )
        events = tuple(_delivery_from_row(dict(row)) for row in cur.fetchall())
        for index, event in enumerate(events, start=1):
            predecessor = events[index - 2] if index > 1 else None
            if (
                event.request.delivery_event_no != index
                or event.request.predecessor_event_hash
                != (
                    predecessor.delivery_event_hash if predecessor is not None else None
                )
            ):
                raise SourceLedgerError(
                    REASON_TRACE_OUTBOX_CHAIN_INVALID,
                    "persisted trace delivery chain is invalid",
                )
            if (
                predecessor is not None
                and predecessor.request.event_type
                is TraceDeliveryEventType.OBSERVATION_WRITTEN
            ):
                raise SourceLedgerError(
                    REASON_TRACE_OUTBOX_CHAIN_INVALID,
                    "written delivery event is terminal",
                )
        return events

    @staticmethod
    def read_delivery_chain_exact_readonly(
        cur: Any, trace_outbox_id: str
    ) -> tuple[TraceDeliveryEvent, ...]:
        cur.execute(
            f"""
            SELECT {_TRACE_DELIVERY_COLUMNS}
            FROM app.advisory_selection_stage_trace_delivery_event
            WHERE trace_outbox_id = %s
            ORDER BY delivery_event_no
            """,
            (trace_outbox_id,),
        )
        events = tuple(_delivery_from_row(dict(row)) for row in cur.fetchall())
        for index, event in enumerate(events, start=1):
            predecessor = events[index - 2] if index > 1 else None
            if (
                event.request.delivery_event_no != index
                or event.request.predecessor_event_hash
                != (
                    predecessor.delivery_event_hash if predecessor is not None else None
                )
            ):
                raise SourceLedgerError(
                    REASON_TRACE_OUTBOX_CHAIN_INVALID,
                    "persisted trace delivery chain is invalid",
                )
            if (
                predecessor is not None
                and predecessor.request.event_type
                is TraceDeliveryEventType.OBSERVATION_WRITTEN
            ):
                raise SourceLedgerError(
                    REASON_TRACE_OUTBOX_CHAIN_INVALID,
                    "written delivery event is terminal",
                )
        return events

    @classmethod
    def append_delivery_in_transaction(
        cls, cur: Any, request: TraceDeliveryEventRequest
    ) -> TraceDeliveryEvent:
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))", (request.trace_outbox_id,)
        )
        cur.execute(
            "SELECT 1 FROM app.advisory_selection_stage_trace_outbox WHERE trace_outbox_id = %s FOR KEY SHARE",
            (request.trace_outbox_id,),
        )
        if cur.fetchone() is None:
            raise SourceLedgerError(
                REASON_TRACE_OUTBOX_RECORD_INVALID, "trace outbox record does not exist"
            )
        chain = cls.read_delivery_chain_exact_in_transaction(
            cur, request.trace_outbox_id
        )
        if request.delivery_event_no <= len(chain):
            existing = chain[request.delivery_event_no - 1]
            if existing.request.request_hash == request.request_hash:
                return existing
            raise SourceLedgerError(
                REASON_TRACE_OUTBOX_CONFLICT,
                "same delivery sequence has a different request",
            )
        if request.delivery_event_no != len(chain) + 1:
            raise SourceLedgerError(
                REASON_TRACE_OUTBOX_CHAIN_INVALID,
                "delivery event number must be the next sequence",
            )
        if chain:
            predecessor = chain[-1]
            if predecessor.delivery_event_hash != request.predecessor_event_hash:
                raise SourceLedgerError(
                    REASON_TRACE_OUTBOX_CHAIN_INVALID,
                    "delivery predecessor does not match prior event",
                )
            if (
                predecessor.request.event_type
                is TraceDeliveryEventType.OBSERVATION_WRITTEN
            ):
                raise SourceLedgerError(
                    REASON_TRACE_OUTBOX_CHAIN_INVALID,
                    "written delivery event is terminal",
                )
        cur.execute("SELECT clock_timestamp() AS event_at")
        event = TraceDeliveryEvent.from_request(
            request, event_at=cur.fetchone()["event_at"]
        )
        try:
            cur.execute(
                f"""
                INSERT INTO app.advisory_selection_stage_trace_delivery_event (
                    delivery_event_id, trace_outbox_id, delivery_event_no, predecessor_event_hash,
                    event_type, writer_attempt_no, event_at, reason_codes, payload_jsonb,
                    delivery_request_hash, delivery_event_hash
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING {_TRACE_DELIVERY_COLUMNS}
                """,
                _delivery_insert_params(event),
            )
        except (psycopg2.IntegrityError, psycopg2.errors.RaiseException) as exc:
            raise SourceLedgerError(
                REASON_TRACE_OUTBOX_CHAIN_INVALID,
                "database rejected trace delivery chain",
            ) from exc
        return _delivery_from_row(dict(cur.fetchone()))

    def contains_identity(self, identity: "ExpectedTraceIdentity") -> bool:
        _require_scope_aware_identity(identity)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                _require_scope_aware_outbox_schema(cur)
                cur.execute(
                    """
                    SELECT 1 FROM app.advisory_selection_stage_trace_outbox
                    WHERE selection_run_id = %s AND package_id = %s AND manifest_sha256 = %s
                      AND decision_as_of_trade_date = %s AND capture_policy_hash = %s
                      AND admission_scope_hash = %s
                    """,
                    identity.natural_key,
                )
                return cur.fetchone() is not None


class LegacyExpectedTraceIdentityV1(BaseModel):
    """Read-only parser for immutable gaps persisted before Phase 1F.2."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    selection_run_id: str = Field(min_length=1, max_length=160)
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    decision_as_of_trade_date: date
    capture_policy_hash: str = Field(min_length=64, max_length=64)

    @field_validator("manifest_sha256", "capture_policy_hash")
    @classmethod
    def _sha256(cls, value: str) -> str:
        if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
            raise ValueError(
                "legacy expected trace hashes must be lowercase sha256 hex"
            )
        return value

    @property
    def natural_key(self) -> tuple[str, str, str, str, str]:
        return (
            self.selection_run_id,
            self.package_id,
            self.manifest_sha256,
            self.decision_as_of_trade_date.isoformat(),
            self.capture_policy_hash,
        )

    @property
    def selection_lookup_key(self) -> tuple[str, str, str, str]:
        return (
            self.selection_run_id,
            self.package_id,
            self.manifest_sha256,
            self.decision_as_of_trade_date.isoformat(),
        )


class ScopeAwareExpectedTraceIdentityV2(BaseModel):
    """Immutable scope-aware business-success identity for new writes."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[TRACE_IDENTITY_SCHEMA_VERSION_V2] = (
        TRACE_IDENTITY_SCHEMA_VERSION_V2
    )
    selection_run_id: str = Field(min_length=1, max_length=160)
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    decision_as_of_trade_date: date
    capture_policy_hash: str = Field(min_length=64, max_length=64)
    admission_scope_id: str = Field(min_length=1, max_length=160)
    admission_scope_hash: str = Field(min_length=64, max_length=64)

    @field_validator("manifest_sha256", "capture_policy_hash", "admission_scope_hash")
    @classmethod
    def _sha256(cls, value: str) -> str:
        if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
            raise ValueError(
                "scope-aware expected trace hashes must be lowercase sha256 hex"
            )
        return value

    @property
    def natural_key(self) -> tuple[str, str, str, str, str, str]:
        return (
            self.selection_run_id,
            self.package_id,
            self.manifest_sha256,
            self.decision_as_of_trade_date.isoformat(),
            self.capture_policy_hash,
            self.admission_scope_hash,
        )

    @property
    def selection_lookup_key(self) -> tuple[str, str, str, str]:
        return (
            self.selection_run_id,
            self.package_id,
            self.manifest_sha256,
            self.decision_as_of_trade_date.isoformat(),
        )

    @classmethod
    def from_envelope(
        cls, envelope: StageTraceEnvelope, *, binding: TraceCaptureBinding
    ) -> "ScopeAwareExpectedTraceIdentityV2":
        selection_identity = envelope.trace_content.get("selection_identity") or {}
        try:
            return cls(
                selection_run_id=str(selection_identity.get("selection_run_id") or ""),
                package_id=str(selection_identity.get("package_id") or ""),
                manifest_sha256=str(selection_identity.get("manifest_sha256") or ""),
                decision_as_of_trade_date=date.fromisoformat(
                    str(selection_identity.get("decision_as_of_trade_date") or "")
                ),
                capture_policy_hash=str(binding.capture_policy.policy_hash or ""),
                admission_scope_id=binding.admission_scope_id,
                admission_scope_hash=binding.admission_scope_hash,
            )
        except (TypeError, ValueError) as exc:
            raise SourceLedgerError(
                REASON_PHASE1F2_TRACE_IDENTITY_INVALID,
                "trace envelope and binding do not form a valid scope-aware identity",
            ) from exc


ExpectedTraceIdentity = ScopeAwareExpectedTraceIdentityV2


def _require_scope_aware_identity(identity: Any) -> ScopeAwareExpectedTraceIdentityV2:
    if not isinstance(identity, ScopeAwareExpectedTraceIdentityV2):
        raise SourceLedgerError(
            REASON_PHASE1F2_TRACE_IDENTITY_INVALID,
            "new trace identity operations require the scope-aware v2 contract",
        )
    return identity


class TraceCaptureGapHandler(Protocol):
    def __call__(
        self, *, identity: ExpectedTraceIdentity, reason_code: str
    ) -> None: ...


class TraceOutboxIdentityLookup(Protocol):
    def contains_identity(self, identity: ExpectedTraceIdentity) -> bool: ...


class TraceCaptureReconciler:
    """Append LOST gaps for business-success identities missing durable outbox rows."""

    def __init__(
        self, *, outbox: TraceOutboxIdentityLookup, gap_handler: TraceCaptureGapHandler
    ) -> None:
        self._outbox = outbox
        self._gap_handler = gap_handler

    def reconcile(
        self, expected: Sequence[ExpectedTraceIdentity]
    ) -> tuple[ExpectedTraceIdentity, ...]:
        missing: list[ExpectedTraceIdentity] = []
        for identity in expected:
            _require_scope_aware_identity(identity)
            if self._outbox.contains_identity(identity):
                continue
            self._gap_handler(identity=identity, reason_code=REASON_TRACE_CAPTURE_LOST)
            missing.append(identity)
        return tuple(missing)


def _natural_key(
    envelope: StageTraceEnvelope, binding: TraceCaptureBinding
) -> tuple[str, str, str, str, str, str]:
    identity = envelope.trace_content.get("selection_identity")
    if not isinstance(identity, Mapping):
        raise SourceLedgerError(
            REASON_TRACE_OUTBOX_RECORD_INVALID,
            "trace content does not contain selection identity",
        )
    return (
        str(identity.get("selection_run_id") or ""),
        str(identity.get("package_id") or ""),
        str(identity.get("manifest_sha256") or ""),
        str(identity.get("decision_as_of_trade_date") or ""),
        str(binding.capture_policy.policy_hash or ""),
        str(binding.admission_scope_hash or ""),
    )


def _validate_envelope_binding(
    *, envelope: StageTraceEnvelope, binding: TraceCaptureBinding
) -> None:
    embedded = envelope.trace_content.get("trace_capture_binding")
    if _canonicalize(embedded) != _canonicalize(binding.model_dump(mode="json")):
        raise SourceLedgerError(
            REASON_TRACE_OUTBOX_RECORD_INVALID,
            "trace envelope binding does not match append binding",
        )
    natural_key = _natural_key(envelope, binding)
    if not all(natural_key):
        raise SourceLedgerError(
            REASON_TRACE_OUTBOX_RECORD_INVALID,
            "trace envelope selection identity is incomplete",
        )


def _binding_semantic_payload(binding: TraceCaptureBinding) -> dict[str, Any]:
    return _canonicalize(
        binding.model_dump(
            mode="json",
            exclude={
                "control_binding_event_hash",
                "capture_batch_id",
                "capture_fencing_token",
                "binding_hash",
            },
        )
    )


def _outbox_insert_params(
    *, envelope: StageTraceEnvelope, binding: TraceCaptureBinding, created_at: datetime
) -> tuple[Any, ...]:
    identity = ExpectedTraceIdentity.from_envelope(envelope, binding=binding)
    return (
        envelope.trace_outbox_id,
        binding.control_binding_event_hash,
        identity.selection_run_id,
        identity.package_id,
        identity.manifest_sha256,
        identity.decision_as_of_trade_date.isoformat(),
        binding.handoff_readiness_hash,
        identity.admission_scope_id,
        identity.admission_scope_hash,
        binding.capture_batch_id,
        binding.capture_fencing_token,
        str(envelope.trace_content.get("schema_version") or ""),
        identity.capture_policy_hash,
        psycopg2.extras.Json(_canonicalize(envelope.trace_content)),
        envelope.trace_content_hash,
        envelope.candidate_count,
        envelope.size_bytes,
        created_at,
    )


def _require_scope_aware_outbox_schema(cur: Any) -> None:
    cur.execute(
        """
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class r ON r.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = 'app'
          AND r.relname = 'advisory_selection_stage_trace_outbox'
          AND c.conname = 'uq_advisory_stage_trace_outbox_scope_identity'
        """
    )
    if cur.fetchone() is None:
        raise SourceLedgerError(
            REASON_PHASE1F2_SCHEMA_NOT_READY,
            "scope-aware trace outbox schema has not been applied",
        )


def _record_from_row(row: dict[str, Any]) -> TraceOutboxRecord:
    content = _canonicalize(dict(row["trace_content_jsonb"]))
    binding_raw = content.get("trace_capture_binding")
    try:
        binding = TraceCaptureBinding.model_validate(binding_raw)
        envelope = StageTraceEnvelope(
            trace_outbox_id=str(row["trace_outbox_id"]),
            trace_content_hash=str(row["trace_content_hash"]),
            trace_content=content,
            candidate_count=int(row["candidate_count"]),
            size_bytes=int(row["size_bytes"]),
        )
        record = TraceOutboxRecord(
            trace_outbox_id=str(row["trace_outbox_id"]),
            envelope=envelope,
            binding=binding,
            created_at=row["created_at"],
        )
    except Exception as exc:
        raise SourceLedgerError(
            REASON_TRACE_OUTBOX_RECORD_INVALID, "persisted trace outbox row is invalid"
        ) from exc
    if (
        binding.control_binding_event_hash != str(row["control_binding_event_hash"])
        or binding.handoff_readiness_hash != str(row["handoff_readiness_hash"])
        or binding.admission_scope_id != str(row["admission_scope_id"])
        or binding.admission_scope_hash != str(row["admission_scope_hash"])
        or binding.capture_batch_id != str(row["capture_batch_id"])
        or binding.capture_fencing_token != int(row["capture_fencing_token"])
        or binding.capture_policy.policy_hash != str(row["capture_policy_hash"])
    ):
        raise SourceLedgerError(
            REASON_TRACE_OUTBOX_RECORD_INVALID,
            "persisted trace outbox columns do not match envelope binding",
        )
    return record


def _delivery_insert_params(event: TraceDeliveryEvent) -> tuple[Any, ...]:
    request = event.request
    return (
        event.delivery_event_id,
        request.trace_outbox_id,
        request.delivery_event_no,
        request.predecessor_event_hash,
        request.event_type.value,
        request.writer_attempt_no,
        event.event_at,
        psycopg2.extras.Json(list(request.reason_codes)),
        psycopg2.extras.Json(_canonicalize(request.payload)),
        request.request_hash,
        event.delivery_event_hash,
    )


def _delivery_from_row(row: dict[str, Any]) -> TraceDeliveryEvent:
    request = TraceDeliveryEventRequest(
        trace_outbox_id=str(row["trace_outbox_id"]),
        delivery_event_no=int(row["delivery_event_no"]),
        predecessor_event_hash=(
            str(row["predecessor_event_hash"])
            if row["predecessor_event_hash"]
            else None
        ),
        event_type=TraceDeliveryEventType(str(row["event_type"])),
        writer_attempt_no=int(row["writer_attempt_no"]),
        reason_codes=tuple(str(code) for code in row["reason_codes"] or []),
        payload=_canonicalize(dict(row["payload_jsonb"])),
    )
    event = TraceDeliveryEvent.from_request(request, event_at=row["event_at"])
    if event.delivery_event_id != str(
        row["delivery_event_id"]
    ) or event.delivery_event_hash != str(row["delivery_event_hash"]):
        raise SourceLedgerError(
            REASON_TRACE_OUTBOX_RECORD_INVALID,
            "persisted delivery event does not match canonical hash",
        )
    if request.request_hash != str(row["delivery_request_hash"]):
        raise SourceLedgerError(
            REASON_TRACE_OUTBOX_RECORD_INVALID,
            "persisted delivery request hash is invalid",
        )
    return event


def _canonicalize(value: Any) -> Any:
    from backend.services.advisory_phase0a.policy import canonicalize

    return canonicalize(value)


def _canonical_json_sha256(value: Any) -> str:
    from backend.services.advisory_phase0a.policy import canonical_json_sha256

    return canonical_json_sha256(value)
