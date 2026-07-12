"""Immutable source-revision sets pinned to exact availability events."""

from __future__ import annotations

from datetime import date, datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.source_ledger import SourceAvailabilityEvent, SourceAvailabilityEventType, SourceLedgerError


SOURCE_REVISION_SET_SCHEMA_VERSION = "advisory_phase1_source_revision_set_v1"
REASON_REVISION_MEMBER_INVALID = "ADVISORY_PHASE1_SOURCE_REVISION_MEMBER_INVALID"
REASON_REVISION_SET_CONFLICT = "ADVISORY_PHASE1_SOURCE_REVISION_SET_CONFLICT"


class SourceRevisionKind(str, Enum):
    IMMUTABLE_INGESTION = "IMMUTABLE_INGESTION"
    PARTITION_CONTENT_HASH = "PARTITION_CONTENT_HASH"
    DURABLE_DB_SNAPSHOT = "DURABLE_DB_SNAPSHOT"
    WATERMARK_ONLY = "WATERMARK_ONLY"


class AvailabilityRequirement(str, Enum):
    DECISION_CUTOFF = "DECISION_CUTOFF"
    LABEL_AS_OF = "LABEL_AS_OF"
    POLICY_FROZEN = "POLICY_FROZEN"


class SourceRevisionMemberInput(BaseModel):
    """One exact source partition consumed by a capture, label or build."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_role: str = Field(min_length=1, max_length=80)
    dataset_name: str = Field(min_length=1, max_length=160)
    query_template_id: str = Field(min_length=1, max_length=160)
    query_template_version: str = Field(min_length=1, max_length=80)
    query_template_hash: str = Field(min_length=64, max_length=64)
    bound_parameter_hash: str = Field(min_length=64, max_length=64)
    partition_key: dict[str, Any] = Field(min_length=1)
    revision_kind: SourceRevisionKind
    revision_id: str = Field(min_length=1, max_length=160)
    availability_requirement: AvailabilityRequirement
    business_min_date: date
    business_max_date: date
    available_at_min: datetime
    available_at_max: datetime
    schema_fingerprint: str = Field(min_length=1, max_length=160)
    row_count: int = Field(ge=0)
    partition_content_hash: str = Field(min_length=64, max_length=64)
    quality_status: str = Field(min_length=1, max_length=32)
    reason_codes: tuple[str, ...] = ()
    availability_event: SourceAvailabilityEvent | None = None
    research_only: bool = True

    @field_validator("available_at_min", "available_at_max")
    @classmethod
    def _require_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("availability timestamps must include an explicit timezone")
        return value.astimezone(timezone.utc)

    @field_validator("query_template_hash", "bound_parameter_hash", "partition_content_hash")
    @classmethod
    def _require_sha256(cls, value: str) -> str:
        if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
            raise ValueError("hash must be lowercase sha256 hex")
        return value

    @model_validator(mode="after")
    def _validate_exact_evidence(self) -> "SourceRevisionMemberInput":
        if self.business_min_date > self.business_max_date or self.available_at_min > self.available_at_max:
            raise ValueError("member intervals must be ordered")
        if not self.research_only:
            raise ValueError("Advisory source revision members must remain research-only")
        event = self.availability_event
        if event is None:
            if self.availability_requirement is AvailabilityRequirement.DECISION_CUTOFF:
                raise ValueError("decision-cutoff research member requires an exact availability event")
            return self
        source = event.input
        if (
            self.dataset_name != source.dataset_name
            or self.source_role != source.source_role
            or self.partition_key_hash != source.partition_key_hash
            or self.revision_id != source.revision_id
            or self.partition_content_hash != source.partition_content_hash
            or self.schema_fingerprint != source.schema_fingerprint
            or self.row_count != source.row_count
            or self.available_at_min != source.formal_available_at
            or self.available_at_max != source.formal_available_at
        ):
            raise ValueError("member fields must exactly match its availability event")
        if source.quality_status != "PASS":
            raise ValueError("research-ready member requires PASS availability-event quality")
        if event.event_type is SourceAvailabilityEventType.INVALIDATED:
            raise ValueError("research member cannot reference an INVALIDATED availability event")
        return self

    @property
    def partition_key_hash(self) -> str:
        return canonical_json_sha256(canonicalize(self.partition_key))

    @property
    def member_key(self) -> str:
        return canonical_json_sha256(
            {
                "source_role": self.source_role,
                "dataset_name": self.dataset_name,
                "query_template_hash": self.query_template_hash,
                "bound_parameter_hash": self.bound_parameter_hash,
                "partition_key_hash": self.partition_key_hash,
                "availability_requirement": self.availability_requirement.value,
            }
        )

    def content_payload(self) -> dict[str, Any]:
        return {
            "source_role": self.source_role,
            "dataset_name": self.dataset_name,
            "query_template_id": self.query_template_id,
            "query_template_version": self.query_template_version,
            "query_template_hash": self.query_template_hash,
            "bound_parameter_hash": self.bound_parameter_hash,
            "partition_key": canonicalize(self.partition_key),
            "partition_key_hash": self.partition_key_hash,
            "revision_kind": self.revision_kind.value,
            "revision_id": self.revision_id,
            "availability_event_hash": self.availability_event.event_content_hash if self.availability_event else None,
            "availability_requirement": self.availability_requirement.value,
            "business_min_date": self.business_min_date,
            "business_max_date": self.business_max_date,
            "available_at_min": self.available_at_min,
            "available_at_max": self.available_at_max,
            "schema_fingerprint": self.schema_fingerprint,
            "row_count": self.row_count,
            "partition_content_hash": self.partition_content_hash,
            "quality_status": self.quality_status,
            "reason_codes": list(self.reason_codes),
            "research_only": self.research_only,
        }


class SourceRevisionSet(BaseModel):
    """Canonical, immutable source set; database snapshots never enter this hash."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_revision_set_id: str
    source_revision_set_hash: str
    query_registry_hash: str
    requested_source_cutoff: datetime
    label_as_of_ts: datetime
    research_only: bool
    members: tuple[SourceRevisionMemberInput, ...]


def build_source_revision_set(
    *,
    query_registry_hash: str,
    requested_source_cutoff: datetime,
    label_as_of_ts: datetime,
    research_only: bool,
    members: list[SourceRevisionMemberInput],
) -> SourceRevisionSet:
    if requested_source_cutoff.tzinfo is None or requested_source_cutoff.utcoffset() is None or label_as_of_ts.tzinfo is None or label_as_of_ts.utcoffset() is None:
        raise SourceLedgerError(REASON_REVISION_MEMBER_INVALID, "revision-set cutoffs must include explicit timezones")
    if len(query_registry_hash) != 64 or any(char not in "0123456789abcdef" for char in query_registry_hash):
        raise SourceLedgerError(REASON_REVISION_MEMBER_INVALID, "query_registry_hash must be lowercase sha256 hex")
    if label_as_of_ts < requested_source_cutoff:
        raise SourceLedgerError(REASON_REVISION_MEMBER_INVALID, "label_as_of_ts cannot precede requested source cutoff")
    if not members:
        raise SourceLedgerError(REASON_REVISION_MEMBER_INVALID, "revision set requires at least one member")
    ordered = tuple(sorted(members, key=lambda member: member.member_key))
    if len({member.member_key for member in ordered}) != len(ordered):
        raise SourceLedgerError(REASON_REVISION_MEMBER_INVALID, "revision set contains duplicate member identities")
    if any(member.research_only != research_only for member in ordered):
        raise SourceLedgerError(REASON_REVISION_MEMBER_INVALID, "members and revision set must use one research-only boundary")
    if not research_only:
        raise SourceLedgerError(REASON_REVISION_MEMBER_INVALID, "Advisory source revision sets must remain research-only")
    for member in ordered:
        if member.availability_event is None:
            continue
        required_cutoff = label_as_of_ts if member.availability_requirement is AvailabilityRequirement.LABEL_AS_OF else requested_source_cutoff
        if member.availability_requirement is AvailabilityRequirement.DECISION_CUTOFF and member.availability_event.formal_available_at > required_cutoff:
            raise SourceLedgerError(
                REASON_REVISION_MEMBER_INVALID,
                "decision-cutoff member was not formally available by requested source cutoff",
                context={"availability_event_hash": member.availability_event.event_content_hash},
            )
        if member.availability_requirement is AvailabilityRequirement.LABEL_AS_OF and member.availability_event.formal_available_at > required_cutoff:
            raise SourceLedgerError(
                REASON_REVISION_MEMBER_INVALID,
                "label member was not formally available by label as-of timestamp",
                context={"availability_event_hash": member.availability_event.event_content_hash},
            )
    payload = {
        "schema_version": SOURCE_REVISION_SET_SCHEMA_VERSION,
        "query_registry_hash": query_registry_hash,
        "requested_source_cutoff": requested_source_cutoff.astimezone(timezone.utc),
        "label_as_of_ts": label_as_of_ts.astimezone(timezone.utc),
        "research_only": research_only,
        "members": [member.content_payload() for member in ordered],
    }
    source_revision_set_hash = canonical_json_sha256(payload)
    return SourceRevisionSet(
        source_revision_set_id=f"srs_{source_revision_set_hash[:20]}",
        source_revision_set_hash=source_revision_set_hash,
        query_registry_hash=query_registry_hash,
        requested_source_cutoff=requested_source_cutoff.astimezone(timezone.utc),
        label_as_of_ts=label_as_of_ts.astimezone(timezone.utc),
        research_only=research_only,
        members=ordered,
    )
