"""Immutable source-availability evidence for Advisory historical research.

The ledger records when a partition was first observed after a successful
ingestion.  It never writes market source tables, guesses an earlier available
time, or starts an observer.  Consumers select an exact event as of a cutoff;
missing, invalidated or ambiguous evidence fails closed.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize


SOURCE_AVAILABILITY_SCHEMA_VERSION = "advisory_phase1_source_availability_v1"
REASON_EVENT_TIME_INVALID = "ADVISORY_PHASE1_SOURCE_EVENT_TIME_INVALID"
REASON_EVENT_CHAIN_INVALID = "ADVISORY_PHASE1_SOURCE_EVENT_CHAIN_INVALID"
REASON_EVENT_CONFLICT = "ADVISORY_PHASE1_SOURCE_EVENT_CONFLICT"
REASON_SOURCE_UNAVAILABLE = "ADVISORY_PHASE1_SOURCE_UNAVAILABLE"
REASON_SOURCE_INVALIDATED = "ADVISORY_PHASE1_SOURCE_INVALIDATED"
REASON_SOURCE_QUALITY_INVALID = "ADVISORY_PHASE1_SOURCE_QUALITY_INVALID"


class SourceAvailabilityEventType(str, Enum):
    INGESTED = "INGESTED"
    CORRECTED = "CORRECTED"
    INVALIDATED = "INVALIDATED"
    REVALIDATED = "REVALIDATED"


class SourceLedgerError(RuntimeError):
    """A fail-closed source-ledger error with a stable machine reason."""

    def __init__(self, reason_code: str, detail: str, *, context: dict[str, Any] | None = None) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.context = context or {}


class SourceAvailabilityEventRequest(BaseModel):
    """Append request without caller-controlled observation time or chain id."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    dataset_name: str = Field(min_length=1, max_length=160)
    source_role: str = Field(min_length=1, max_length=80)
    partition_key: dict[str, Any] = Field(min_length=1)
    revision_id: str = Field(min_length=1, max_length=160)
    event_revision_no: int = Field(ge=1)
    event_type: SourceAvailabilityEventType
    predecessor_event_hash: str | None = Field(default=None, min_length=64, max_length=64)
    provider_job_id: str | None = Field(default=None, max_length=160)
    refresh_job_id: str | None = Field(default=None, max_length=160)
    provider_published_at: datetime | None = None
    schema_fingerprint: str = Field(min_length=1, max_length=160)
    row_count: int = Field(ge=0)
    partition_content_hash: str = Field(min_length=64, max_length=64)
    quality_status: str = Field(min_length=1, max_length=32)
    reason_codes: tuple[str, ...] = ()
    created_by_service_principal: str = Field(min_length=1, max_length=160)

    @field_validator("provider_published_at")
    @classmethod
    def _require_aware_timestamp(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return value
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must include an explicit timezone")
        return value.astimezone(timezone.utc)

    @field_validator("predecessor_event_hash", "partition_content_hash")
    @classmethod
    def _require_sha256(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
            raise ValueError("hash must be lowercase sha256 hex")
        return value

    @model_validator(mode="after")
    def _validate_shape(self) -> "SourceAvailabilityEventRequest":
        if self.event_revision_no == 1:
            if self.event_type is not SourceAvailabilityEventType.INGESTED or self.predecessor_event_hash is not None:
                raise ValueError("revision one must be INGESTED without a predecessor")
        elif self.predecessor_event_hash is None:
            raise ValueError("non-first revision requires predecessor_event_hash")
        return self

    @property
    def partition_key_hash(self) -> str:
        return canonical_json_sha256(canonicalize(self.partition_key))

    @property
    def derived_partition_chain_key(self) -> str:
        return canonical_json_sha256(
            {
                "dataset_name": self.dataset_name,
                "source_role": self.source_role,
                "partition_key_hash": self.partition_key_hash,
            }
        )

    @property
    def derived_append_request_hash(self) -> str:
        return canonical_json_sha256(self.request_payload())

    def request_payload(self) -> dict[str, Any]:
        return {
            "schema_version": SOURCE_AVAILABILITY_SCHEMA_VERSION,
            "dataset_name": self.dataset_name,
            "source_role": self.source_role,
            "partition_key": canonicalize(self.partition_key),
            "partition_key_hash": self.partition_key_hash,
            "partition_chain_key": self.derived_partition_chain_key,
            "revision_id": self.revision_id,
            "event_revision_no": self.event_revision_no,
            "event_type": self.event_type.value,
            "predecessor_event_hash": self.predecessor_event_hash,
            "provider_job_id": self.provider_job_id,
            "refresh_job_id": self.refresh_job_id,
            "provider_published_at": self.provider_published_at,
            "schema_fingerprint": self.schema_fingerprint,
            "row_count": self.row_count,
            "partition_content_hash": self.partition_content_hash,
            "quality_status": self.quality_status,
            "reason_codes": list(self.reason_codes),
            "created_by_service_principal": self.created_by_service_principal,
        }

    def materialize(self, *, first_observed_at: datetime) -> "SourceAvailabilityEventInput":
        return SourceAvailabilityEventInput(
            **self.model_dump(),
            partition_chain_key=self.derived_partition_chain_key,
            append_request_hash=self.derived_append_request_hash,
            first_observed_at=first_observed_at,
        )


class SourceAvailabilityEventInput(SourceAvailabilityEventRequest):
    """Canonical event payload after repository-controlled observation time."""

    partition_chain_key: str = Field(min_length=64, max_length=64)
    append_request_hash: str = Field(min_length=64, max_length=64)
    first_observed_at: datetime

    @field_validator("first_observed_at")
    @classmethod
    def _require_observation_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must include an explicit timezone")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def _validate_repository_fields(self) -> "SourceAvailabilityEventInput":
        request = SourceAvailabilityEventRequest.model_validate(
            self.model_dump(exclude={"partition_chain_key", "append_request_hash", "first_observed_at"})
        )
        if (
            self.partition_chain_key != request.derived_partition_chain_key
            or self.append_request_hash != request.derived_append_request_hash
        ):
            raise ValueError("repository-controlled chain or request hash is invalid")
        return self

    @property
    def formal_available_at(self) -> datetime:
        return max(timestamp for timestamp in (self.provider_published_at, self.first_observed_at) if timestamp is not None)

    def content_payload(self) -> dict[str, Any]:
        return {
            "schema_version": SOURCE_AVAILABILITY_SCHEMA_VERSION,
            "dataset_name": self.dataset_name,
            "source_role": self.source_role,
            "partition_key": canonicalize(self.partition_key),
            "partition_key_hash": self.partition_key_hash,
            "partition_chain_key": self.partition_chain_key,
            "append_request_hash": self.append_request_hash,
            "revision_id": self.revision_id,
            "event_revision_no": self.event_revision_no,
            "event_type": self.event_type.value,
            "predecessor_event_hash": self.predecessor_event_hash,
            "provider_job_id": self.provider_job_id,
            "refresh_job_id": self.refresh_job_id,
            "provider_published_at": self.provider_published_at,
            "first_observed_at": self.first_observed_at,
            "formal_available_at": self.formal_available_at,
            "schema_fingerprint": self.schema_fingerprint,
            "row_count": self.row_count,
            "partition_content_hash": self.partition_content_hash,
            "quality_status": self.quality_status,
            "reason_codes": list(self.reason_codes),
            "created_by_service_principal": self.created_by_service_principal,
        }


class SourceAvailabilityEvent(BaseModel):
    """Canonical immutable availability event."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    availability_event_id: str
    event_content_hash: str
    input: SourceAvailabilityEventInput

    @property
    def partition_chain_key(self) -> str:
        return self.input.partition_chain_key

    @property
    def event_revision_no(self) -> int:
        return self.input.event_revision_no

    @property
    def event_type(self) -> SourceAvailabilityEventType:
        return self.input.event_type

    @property
    def formal_available_at(self) -> datetime:
        return self.input.formal_available_at

    @classmethod
    def from_input(cls, item: SourceAvailabilityEventInput) -> "SourceAvailabilityEvent":
        event_content_hash = canonical_json_sha256(item.content_payload())
        return cls(
            availability_event_id=f"ase_{event_content_hash[:20]}",
            event_content_hash=event_content_hash,
            input=item,
        )

    @classmethod
    def from_request(
        cls,
        request: SourceAvailabilityEventRequest,
        *,
        first_observed_at: datetime,
    ) -> "SourceAvailabilityEvent":
        return cls.from_input(request.materialize(first_observed_at=first_observed_at))

    def source_evidence_payload(self) -> dict[str, Any]:
        """Exact immutable projection for a later source-revision member."""
        return {
            "availability_event_id": self.availability_event_id,
            "availability_event_hash": self.event_content_hash,
            "dataset_name": self.input.dataset_name,
            "source_role": self.input.source_role,
            "partition_key_hash": self.input.partition_key_hash,
            "partition_chain_key": self.partition_chain_key,
            "revision_id": self.input.revision_id,
            "event_revision_no": self.event_revision_no,
            "event_type": self.event_type.value,
            "formal_available_at": self.formal_available_at,
            "schema_fingerprint": self.input.schema_fingerprint,
            "row_count": self.input.row_count,
            "partition_content_hash": self.input.partition_content_hash,
        }


class InMemorySourceAvailabilityLedger:
    """Deterministic ledger oracle used by Phase 1 repository implementations."""

    def __init__(self, *, now_provider: Callable[[], datetime] | None = None) -> None:
        self._events_by_hash: dict[str, SourceAvailabilityEvent] = {}
        self._events_by_chain: dict[str, list[SourceAvailabilityEvent]] = {}
        self._successor_by_hash: dict[str, str] = {}
        self._now_provider = now_provider or (lambda: datetime.now(timezone.utc))

    def append(self, request: SourceAvailabilityEventRequest) -> SourceAvailabilityEvent:
        chain = self._events_by_chain.setdefault(request.derived_partition_chain_key, [])
        if request.event_revision_no <= len(chain):
            existing = chain[request.event_revision_no - 1]
            if existing.input.append_request_hash == request.derived_append_request_hash:
                return existing
            raise SourceLedgerError(
                REASON_EVENT_CONFLICT,
                "same natural partition revision has a different append request",
                context={"partition_chain_key": request.derived_partition_chain_key, "event_revision_no": request.event_revision_no},
            )
        if request.event_revision_no != len(chain) + 1:
            raise SourceLedgerError(
                REASON_EVENT_CHAIN_INVALID,
                "event_revision_no must be the next sequence number for its partition chain",
                context={"partition_chain_key": request.derived_partition_chain_key, "event_revision_no": request.event_revision_no},
            )
        event = SourceAvailabilityEvent.from_request(request, first_observed_at=self._now_provider())
        if chain:
            self._validate_successor(event=event, predecessor=chain[-1])
        chain.append(event)
        self._events_by_hash[event.event_content_hash] = event
        if request.predecessor_event_hash is not None:
            self._successor_by_hash[request.predecessor_event_hash] = event.event_content_hash
        return event

    def select_as_of(
        self,
        *,
        dataset_name: str,
        source_role: str,
        partition_key: dict[str, Any],
        cutoff: datetime,
    ) -> SourceAvailabilityEvent:
        if cutoff.tzinfo is None or cutoff.utcoffset() is None:
            raise SourceLedgerError(REASON_EVENT_TIME_INVALID, "cutoff must include an explicit timezone")
        partition_chain_key = source_partition_chain_key(
            dataset_name=dataset_name,
            source_role=source_role,
            partition_key=partition_key,
        )
        candidates = [
            event
            for event in self._events_by_chain.get(partition_chain_key, [])
            if event.formal_available_at <= cutoff.astimezone(timezone.utc)
        ]
        if not candidates:
            raise SourceLedgerError(
                REASON_SOURCE_UNAVAILABLE,
                "no source availability event was formally available by cutoff",
                context={"partition_chain_key": partition_chain_key, "cutoff": cutoff.isoformat()},
            )
        terminal = max(candidates, key=lambda event: event.event_revision_no)
        if terminal.event_type is SourceAvailabilityEventType.INVALIDATED:
            raise SourceLedgerError(
                REASON_SOURCE_INVALIDATED,
                "latest source event available by cutoff is invalidated",
                context={"availability_event_hash": terminal.event_content_hash},
            )
        if terminal.input.quality_status != "PASS":
            raise SourceLedgerError(
                REASON_SOURCE_QUALITY_INVALID,
                "latest source event available by cutoff does not have PASS quality",
                context={"availability_event_hash": terminal.event_content_hash, "quality_status": terminal.input.quality_status},
            )
        return terminal

    def _validate_successor(self, *, event: SourceAvailabilityEvent, predecessor: SourceAvailabilityEvent) -> None:
        item = event.input
        if item.predecessor_event_hash != predecessor.event_content_hash:
            raise SourceLedgerError(REASON_EVENT_CHAIN_INVALID, "predecessor must be the prior event in the same chain")
        if item.partition_key_hash != predecessor.input.partition_key_hash:
            raise SourceLedgerError(REASON_EVENT_CHAIN_INVALID, "successor partition key differs from predecessor")
        if predecessor.event_content_hash in self._successor_by_hash:
            raise SourceLedgerError(REASON_EVENT_CHAIN_INVALID, "predecessor already has a successor")
        if item.event_type is SourceAvailabilityEventType.REVALIDATED:
            if predecessor.event_type is not SourceAvailabilityEventType.INVALIDATED:
                raise SourceLedgerError(REASON_EVENT_CHAIN_INVALID, "REVALIDATED requires an INVALIDATED predecessor")
        elif predecessor.event_type is SourceAvailabilityEventType.INVALIDATED:
            raise SourceLedgerError(REASON_EVENT_CHAIN_INVALID, "INVALIDATED predecessor requires REVALIDATED successor")
        if item.event_type in {SourceAvailabilityEventType.CORRECTED, SourceAvailabilityEventType.REVALIDATED}:
            if item.revision_id == predecessor.input.revision_id or item.partition_content_hash == predecessor.input.partition_content_hash:
                raise SourceLedgerError(REASON_EVENT_CHAIN_INVALID, "corrected or revalidated event requires new revision and content hash")


def source_partition_chain_key(*, dataset_name: str, source_role: str, partition_key: dict[str, Any]) -> str:
    return canonical_json_sha256(
        {
            "dataset_name": dataset_name,
            "source_role": source_role,
            "partition_key_hash": canonical_json_sha256(canonicalize(partition_key)),
        }
    )
