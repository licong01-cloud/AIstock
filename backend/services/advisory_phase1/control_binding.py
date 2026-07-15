"""Append-only Phase 1 control bindings, without approvals or role state.

Control bindings version the configuration that can enable an optional Phase 1
sidecar.  They are not a permission system: a new event deterministically
replaces the current configuration for one natural control chain, and every
event remains immutable for later provenance checks.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Iterator

import psycopg2
import psycopg2.extras
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase1.source_ledger import SourceLedgerError


CONTROL_BINDING_SCHEMA_VERSION = "advisory_phase1_control_binding_v1"
REASON_CONTROL_BINDING_CONFLICT = "ADVISORY_PHASE1_CONTROL_BINDING_CONFLICT"
REASON_CONTROL_BINDING_CHAIN_INVALID = "ADVISORY_PHASE1_CONTROL_BINDING_CHAIN_INVALID"
REASON_CONTROL_BINDING_UNAVAILABLE = "ADVISORY_PHASE1_CONTROL_BINDING_UNAVAILABLE"


class ControlType(str, Enum):
    TRACE_CAPTURE = "TRACE_CAPTURE"
    SOURCE_LEDGER_OBSERVER = "SOURCE_LEDGER_OBSERVER"
    DATASET_STORE = "DATASET_STORE"
    SCHEDULER = "SCHEDULER"


class ControlBindingRequest(BaseModel):
    """One versioned configuration without caller-controlled event time/chain key."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    control_type: ControlType
    environment: str = Field(min_length=1, max_length=80)
    admission_scope_set_hash: str | None = Field(
        default=None, min_length=64, max_length=64
    )
    governance_scope_hash: str | None = Field(
        default=None, min_length=64, max_length=64
    )
    config_source: str = Field(min_length=1, max_length=160)
    config_payload: dict[str, Any]
    config_or_store_backend_hash: str = Field(min_length=64, max_length=64)
    enabled: bool
    binding_event_revision_no: int = Field(ge=1)
    predecessor_binding_event_hash: str | None = Field(
        default=None, min_length=64, max_length=64
    )
    created_by_service_principal: str = Field(min_length=1, max_length=160)

    @field_validator(
        "admission_scope_set_hash",
        "governance_scope_hash",
        "config_or_store_backend_hash",
        "predecessor_binding_event_hash",
    )
    @classmethod
    def _sha256(cls, value: str | None) -> str | None:
        if value is not None and (
            len(value) != 64 or any(char not in "0123456789abcdef" for char in value)
        ):
            raise ValueError("control binding hash fields must be lowercase sha256 hex")
        return value

    @model_validator(mode="after")
    def _validate_shape(self) -> "ControlBindingRequest":
        if (
            self.control_type is ControlType.TRACE_CAPTURE
            and not self.admission_scope_set_hash
        ):
            raise ValueError("TRACE_CAPTURE binding requires admission_scope_set_hash")
        if (
            self.binding_event_revision_no == 1
            and self.predecessor_binding_event_hash is not None
        ):
            raise ValueError("first control binding revision cannot have a predecessor")
        if (
            self.binding_event_revision_no > 1
            and self.predecessor_binding_event_hash is None
        ):
            raise ValueError(
                "non-first control binding revision requires predecessor_binding_event_hash"
            )
        if (
            _canonical_json_sha256(self.config_payload)
            != self.config_or_store_backend_hash
        ):
            raise ValueError(
                "config_or_store_backend_hash does not match config_payload"
            )
        return self

    @property
    def binding_chain_key(self) -> str:
        return _canonical_json_sha256(
            {
                "control_type": self.control_type.value,
                "environment": self.environment,
                "admission_scope_set_hash": self.admission_scope_set_hash,
                "governance_scope_hash": self.governance_scope_hash,
            }
        )

    @property
    def append_request_hash(self) -> str:
        return _canonical_json_sha256(self.request_payload())

    def request_payload(self) -> dict[str, Any]:
        return {
            "schema_version": CONTROL_BINDING_SCHEMA_VERSION,
            "control_type": self.control_type.value,
            "environment": self.environment,
            "admission_scope_set_hash": self.admission_scope_set_hash,
            "governance_scope_hash": self.governance_scope_hash,
            "config_source": self.config_source,
            "config_payload": _canonicalize(self.config_payload),
            "config_or_store_backend_hash": self.config_or_store_backend_hash,
            "enabled": self.enabled,
            "binding_chain_key": self.binding_chain_key,
            "binding_event_revision_no": self.binding_event_revision_no,
            "predecessor_binding_event_hash": self.predecessor_binding_event_hash,
            "created_by_service_principal": self.created_by_service_principal,
        }


class ControlBindingEvent(BaseModel):
    """Canonical immutable persisted control binding event."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    binding_event_id: str
    binding_event_hash: str
    request: ControlBindingRequest
    bound_at: datetime

    @field_validator("bound_at")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("bound_at must be timezone-aware")
        return value.astimezone(timezone.utc)

    @classmethod
    def from_request(
        cls, request: ControlBindingRequest, *, bound_at: datetime
    ) -> "ControlBindingEvent":
        payload = {
            **request.request_payload(),
            "bound_at": bound_at.astimezone(timezone.utc),
        }
        digest = _canonical_json_sha256(payload)
        return cls(
            binding_event_id=f"cbe_{digest[:20]}",
            binding_event_hash=digest,
            request=request,
            bound_at=bound_at,
        )


class InMemoryControlBindingRepository:
    """Deterministic chain oracle for typed control binding contracts."""

    def __init__(self, *, now_provider: Callable[[], datetime] | None = None) -> None:
        self._events_by_chain: dict[str, list[ControlBindingEvent]] = {}
        self._now_provider = now_provider or (lambda: datetime.now(timezone.utc))

    def append(self, request: ControlBindingRequest) -> ControlBindingEvent:
        chain = self._events_by_chain.setdefault(request.binding_chain_key, [])
        if request.binding_event_revision_no <= len(chain):
            existing = chain[request.binding_event_revision_no - 1]
            if existing.request.append_request_hash == request.append_request_hash:
                return existing
            raise SourceLedgerError(
                REASON_CONTROL_BINDING_CONFLICT,
                "same control binding revision has different content",
            )
        if request.binding_event_revision_no != len(chain) + 1:
            raise SourceLedgerError(
                REASON_CONTROL_BINDING_CHAIN_INVALID,
                "control binding revision is not the next sequence",
            )
        event = ControlBindingEvent.from_request(request, bound_at=self._now_provider())
        if chain:
            predecessor = chain[-1]
            if request.predecessor_binding_event_hash != predecessor.binding_event_hash:
                raise SourceLedgerError(
                    REASON_CONTROL_BINDING_CHAIN_INVALID,
                    "control binding predecessor does not match",
                )
            if (
                predecessor.request.config_or_store_backend_hash
                == request.config_or_store_backend_hash
                and predecessor.request.enabled == request.enabled
                and predecessor.request.config_source == request.config_source
            ):
                raise SourceLedgerError(
                    REASON_CONTROL_BINDING_CONFLICT,
                    "new control binding revision cannot repeat predecessor content",
                )
        chain.append(event)
        return event

    def current(
        self,
        *,
        control_type: ControlType,
        environment: str,
        admission_scope_set_hash: str | None,
        governance_scope_hash: str | None,
    ) -> ControlBindingEvent:
        key = _control_chain_key(
            control_type=control_type,
            environment=environment,
            admission_scope_set_hash=admission_scope_set_hash,
            governance_scope_hash=governance_scope_hash,
        )
        chain = self._events_by_chain.get(key, [])
        if not chain:
            raise SourceLedgerError(
                REASON_CONTROL_BINDING_UNAVAILABLE,
                "control binding chain is not configured",
            )
        return chain[-1]


ConnFactory = Callable[[], Iterator[Any]]


def _transactional_conn_factory() -> Iterator[Any]:
    from backend.db.pg_pool import get_conn

    return get_conn(autocommit=False, manage_transaction=True)


_CONTROL_BINDING_COLUMNS = """
binding_event_id, append_request_hash, control_type, environment,
admission_scope_set_hash, governance_scope_hash, config_source,
config_payload_jsonb, config_or_store_backend_hash, enabled,
binding_chain_key, binding_event_revision_no, predecessor_binding_event_hash,
bound_at, binding_event_hash, created_by_service_principal
"""


class PostgresControlBindingRepository:
    """PostgreSQL append/select repository for immutable control binding chains."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or _transactional_conn_factory

    def append(self, request: ControlBindingRequest) -> ControlBindingEvent:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                return self.append_in_transaction(cur, request)

    def get_by_hash(self, binding_event_hash: str) -> ControlBindingEvent:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                return self.read_exact_in_transaction(cur, binding_event_hash)

    @staticmethod
    def current_in_transaction(cur: Any, binding_chain_key: str) -> ControlBindingEvent:
        cur.execute(
            f"""
            SELECT {_CONTROL_BINDING_COLUMNS}
            FROM app.advisory_phase1_control_binding_event
            WHERE binding_chain_key = %s
            ORDER BY binding_event_revision_no DESC
            LIMIT 1
            FOR UPDATE
            """,
            (binding_chain_key,),
        )
        row = cur.fetchone()
        if row is None:
            raise SourceLedgerError(
                REASON_CONTROL_BINDING_UNAVAILABLE,
                "control binding chain is not configured",
            )
        return _event_from_row(dict(row))

    @staticmethod
    def current_readonly(cur: Any, binding_chain_key: str) -> ControlBindingEvent:
        cur.execute(
            f"""
            SELECT {_CONTROL_BINDING_COLUMNS}
            FROM app.advisory_phase1_control_binding_event
            WHERE binding_chain_key = %s
            ORDER BY binding_event_revision_no DESC
            LIMIT 1
            """,
            (binding_chain_key,),
        )
        row = cur.fetchone()
        if row is None:
            raise SourceLedgerError(
                REASON_CONTROL_BINDING_UNAVAILABLE,
                "control binding chain is not configured",
            )
        return _event_from_row(dict(row))

    @staticmethod
    def read_exact_in_transaction(
        cur: Any, binding_event_hash: str
    ) -> ControlBindingEvent:
        cur.execute(
            f"""
            SELECT {_CONTROL_BINDING_COLUMNS}
            FROM app.advisory_phase1_control_binding_event
            WHERE binding_event_hash = %s
            FOR KEY SHARE
            """,
            (binding_event_hash,),
        )
        row = cur.fetchone()
        if row is None:
            raise SourceLedgerError(
                REASON_CONTROL_BINDING_UNAVAILABLE,
                "control binding event does not exist",
            )
        return _event_from_row(dict(row))

    @staticmethod
    def read_exact_readonly(cur: Any, binding_event_hash: str) -> ControlBindingEvent:
        cur.execute(
            f"""
            SELECT {_CONTROL_BINDING_COLUMNS}
            FROM app.advisory_phase1_control_binding_event
            WHERE binding_event_hash = %s
            """,
            (binding_event_hash,),
        )
        row = cur.fetchone()
        if row is None:
            raise SourceLedgerError(
                REASON_CONTROL_BINDING_UNAVAILABLE,
                "control binding event does not exist",
            )
        return _event_from_row(dict(row))

    @staticmethod
    def append_in_transaction(
        cur: Any, request: ControlBindingRequest
    ) -> ControlBindingEvent:
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))", (request.binding_chain_key,)
        )
        cur.execute(
            f"""
            SELECT {_CONTROL_BINDING_COLUMNS}
            FROM app.advisory_phase1_control_binding_event
            WHERE binding_chain_key = %s AND binding_event_revision_no = %s
            FOR UPDATE
            """,
            (request.binding_chain_key, request.binding_event_revision_no),
        )
        existing = cur.fetchone()
        if existing is not None:
            event = _event_from_row(dict(existing))
            if event.request.append_request_hash == request.append_request_hash:
                return event
            raise SourceLedgerError(
                REASON_CONTROL_BINDING_CONFLICT,
                "same control binding revision has different content",
            )
        cur.execute("SELECT clock_timestamp() AS bound_at")
        event = ControlBindingEvent.from_request(
            request, bound_at=cur.fetchone()["bound_at"]
        )
        try:
            cur.execute(
                f"""
                INSERT INTO app.advisory_phase1_control_binding_event (
                    binding_event_id, append_request_hash, control_type, environment,
                    admission_scope_set_hash, governance_scope_hash, config_source,
                    config_payload_jsonb, config_or_store_backend_hash, enabled,
                    binding_chain_key, binding_event_revision_no, predecessor_binding_event_hash,
                    bound_at, binding_event_hash, created_by_service_principal
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING {_CONTROL_BINDING_COLUMNS}
                """,
                _insert_params(event),
            )
        except (psycopg2.IntegrityError, psycopg2.errors.RaiseException) as exc:
            raise SourceLedgerError(
                REASON_CONTROL_BINDING_CHAIN_INVALID,
                "database rejected control binding chain",
            ) from exc
        return _event_from_row(dict(cur.fetchone()))

    @classmethod
    def get_or_append_exact_in_transaction(
        cls, cur: Any, desired_config: ControlBindingRequest
    ) -> ControlBindingEvent:
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (desired_config.binding_chain_key,),
        )
        try:
            current = cls.current_in_transaction(cur, desired_config.binding_chain_key)
        except SourceLedgerError as exc:
            if exc.reason_code != REASON_CONTROL_BINDING_UNAVAILABLE:
                raise
            first_payload = desired_config.model_dump(mode="python")
            first_payload.update(
                binding_event_revision_no=1, predecessor_binding_event_hash=None
            )
            return cls.append_in_transaction(
                cur, ControlBindingRequest.model_validate(first_payload)
            )
        current_request = current.request
        if (
            current_request.config_source == desired_config.config_source
            and current_request.config_or_store_backend_hash
            == desired_config.config_or_store_backend_hash
            and current_request.enabled is desired_config.enabled
        ):
            return current
        next_payload = desired_config.model_dump(mode="python")
        next_payload.update(
            binding_event_revision_no=current_request.binding_event_revision_no + 1,
            predecessor_binding_event_hash=current.binding_event_hash,
        )
        return cls.append_in_transaction(
            cur, ControlBindingRequest.model_validate(next_payload)
        )


def _control_chain_key(
    *,
    control_type: ControlType,
    environment: str,
    admission_scope_set_hash: str | None,
    governance_scope_hash: str | None,
) -> str:
    return _canonical_json_sha256(
        {
            "control_type": control_type.value,
            "environment": environment,
            "admission_scope_set_hash": admission_scope_set_hash,
            "governance_scope_hash": governance_scope_hash,
        }
    )


def _insert_params(event: ControlBindingEvent) -> tuple[Any, ...]:
    request = event.request
    return (
        event.binding_event_id,
        request.append_request_hash,
        request.control_type.value,
        request.environment,
        request.admission_scope_set_hash,
        request.governance_scope_hash,
        request.config_source,
        psycopg2.extras.Json(_canonicalize(request.config_payload)),
        request.config_or_store_backend_hash,
        request.enabled,
        request.binding_chain_key,
        request.binding_event_revision_no,
        request.predecessor_binding_event_hash,
        event.bound_at,
        event.binding_event_hash,
        request.created_by_service_principal,
    )


def _event_from_row(row: dict[str, Any]) -> ControlBindingEvent:
    request = ControlBindingRequest(
        control_type=ControlType(str(row["control_type"])),
        environment=str(row["environment"]),
        admission_scope_set_hash=(
            str(row["admission_scope_set_hash"])
            if row["admission_scope_set_hash"]
            else None
        ),
        governance_scope_hash=(
            str(row["governance_scope_hash"]) if row["governance_scope_hash"] else None
        ),
        config_source=str(row["config_source"]),
        config_payload=_canonicalize(dict(row["config_payload_jsonb"])),
        config_or_store_backend_hash=str(row["config_or_store_backend_hash"]),
        enabled=bool(row["enabled"]),
        binding_event_revision_no=int(row["binding_event_revision_no"]),
        predecessor_binding_event_hash=(
            str(row["predecessor_binding_event_hash"])
            if row["predecessor_binding_event_hash"]
            else None
        ),
        created_by_service_principal=str(row["created_by_service_principal"]),
    )
    event = ControlBindingEvent.from_request(request, bound_at=row["bound_at"])
    if event.binding_event_id != str(
        row["binding_event_id"]
    ) or event.binding_event_hash != str(row["binding_event_hash"]):
        raise SourceLedgerError(
            REASON_CONTROL_BINDING_CONFLICT,
            "persisted control binding event does not match canonical hash",
        )
    if request.binding_chain_key != str(
        row["binding_chain_key"]
    ) or request.append_request_hash != str(row["append_request_hash"]):
        raise SourceLedgerError(
            REASON_CONTROL_BINDING_CONFLICT,
            "persisted control binding hashes are invalid",
        )
    return event


def _canonicalize(value: Any) -> Any:
    from backend.services.advisory_phase0a.policy import canonicalize

    return canonicalize(value)


def _canonical_json_sha256(value: Any) -> str:
    from backend.services.advisory_phase0a.policy import canonical_json_sha256

    return canonical_json_sha256(value)
