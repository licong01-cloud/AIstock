"""Fixture-only Phase 1C-2 source requirement and exact revision resolution.

The resolver consumes only caller-supplied immutable availability events.  It
does not query a current source table, start an observer, or retry a different
source revision when the as-of terminal is unavailable.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, Iterable, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.source_ledger import (
    REASON_EVENT_CHAIN_INVALID,
    SourceAvailabilityEvent,
    SourceAvailabilityEventType,
    SourceLedgerError,
    source_partition_chain_key,
    validate_source_availability_event_chain,
)
from backend.services.advisory_phase1.source_revision import (
    AvailabilityRequirement,
    SourceRevisionKind,
    SourceRevisionMemberInput,
    SourceRevisionSet,
    build_source_revision_set,
)


SOURCE_REQUIREMENT_SCHEMA_VERSION = "advisory_phase1_source_requirement_v1"
SOURCE_REQUIREMENT_SET_SCHEMA_VERSION = "advisory_phase1_source_requirement_set_v1"
SOURCE_RESOLUTION_RECEIPT_SCHEMA_VERSION = "advisory_phase1_source_resolution_receipt_v1"
SOURCE_RESOLUTION_POLICY_VERSION = "advisory_phase1_fixture_source_resolution_policy_v1"
SOURCE_RESOLUTION_POLICY_HASH = canonical_json_sha256(
    {
        "schema_version": SOURCE_RESOLUTION_POLICY_VERSION,
        "terminal_selection": "unique_max_event_revision_no_as_of_formal_available_at",
        "unavailable": ["MISSING", "INVALIDATED", "QUALITY_NOT_PASS"],
        "conflict": ["CHAIN", "IDENTITY", "HASH", "MEMBER"],
        "fallback": "forbidden",
    }
)

REASON_SOURCE_REQUIREMENT_INVALID = "ADVISORY_PHASE1_SOURCE_REQUIREMENT_INVALID"
REASON_SOURCE_REQUIREMENT_CONFLICT = "ADVISORY_PHASE1_SOURCE_REQUIREMENT_CONFLICT"
REASON_SOURCE_UNAVAILABLE_AS_OF = "ADVISORY_PHASE1_SOURCE_UNAVAILABLE_AS_OF"
REASON_SOURCE_CHAIN_INVALID = "ADVISORY_PHASE1_SOURCE_CHAIN_INVALID"
REASON_SOURCE_TERMINAL_CONFLICT = "ADVISORY_PHASE1_SOURCE_TERMINAL_CONFLICT"
REASON_SOURCE_TERMINAL_INVALIDATED = "ADVISORY_PHASE1_SOURCE_TERMINAL_INVALIDATED"
REASON_SOURCE_QUALITY_INVALID = "ADVISORY_PHASE1_SOURCE_QUALITY_INVALID"
REASON_SOURCE_MEMBER_MISMATCH = "ADVISORY_PHASE1_SOURCE_MEMBER_MISMATCH"
REASON_SOURCE_RESOLUTION_CONFLICT = "ADVISORY_PHASE1_SOURCE_RESOLUTION_CONFLICT"
REASON_SOURCE_REPLAY_NOT_ELIGIBLE = "ADVISORY_PHASE1_SOURCE_REPLAY_NOT_ELIGIBLE"
REASON_SOURCE_REQUIREMENT_SET_CONFLICT = "ADVISORY_PHASE1_SOURCE_REQUIREMENT_SET_CONFLICT"
REASON_SOURCE_RESOLUTION_RECEIPT_CONFLICT = "ADVISORY_PHASE1_SOURCE_RESOLUTION_RECEIPT_CONFLICT"


class RequirementResolutionStatus(str, Enum):
    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"
    CONFLICT = "CONFLICT"


class ResearchReadiness(str, Enum):
    RESEARCH_READY = "RESEARCH_READY"
    PARTIAL = "PARTIAL"
    BLOCKED = "BLOCKED"


def _require_sha256(value: str, *, field_name: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{field_name} must be lowercase sha256 hex")
    return value


def _require_aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include an explicit timezone")
    return value.astimezone(timezone.utc)


def _normalized_reason_codes(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({str(value) for value in values if str(value)}))


def build_source_requirement_common_pit_identity_hash(
    *,
    admission_scope_id: str,
    admission_scope_hash: str,
    handoff_readiness_hash: str,
    program_id: str,
    binding_version_id: str,
    package_id: str,
    manifest_sha256: str,
    alpha_mode: str,
    decision_as_of_trade_date: date,
    requested_source_cutoff: datetime,
    query_registry_hash: str,
    calendar_hash: str,
    universe_policy_hash: str,
    data_source: str,
    execution_origin: str,
    research_scope: str,
    execution_prohibited: bool,
    research_only: bool,
) -> str:
    """Hash only the cross-component PIT identity, never a leg's lookback/window."""

    return canonical_json_sha256(
        {
            "admission_scope_id": admission_scope_id,
            "admission_scope_hash": admission_scope_hash,
            "handoff_readiness_hash": handoff_readiness_hash,
            "program_id": program_id,
            "binding_version_id": binding_version_id,
            "package_id": package_id,
            "manifest_sha256": manifest_sha256,
            "alpha_mode": alpha_mode,
            "decision_as_of_trade_date": decision_as_of_trade_date,
            "requested_source_cutoff": _require_aware(
                requested_source_cutoff,
                field_name="requested_source_cutoff",
            ),
            "query_registry_hash": query_registry_hash,
            "calendar_hash": calendar_hash,
            "universe_policy_hash": universe_policy_hash,
            "data_source": data_source,
            "execution_origin": execution_origin,
            "research_scope": research_scope,
            "execution_prohibited": execution_prohibited,
            "research_only": research_only,
        }
    )


class SourceRequirement(BaseModel):
    """One explicit, immutable source requirement for a research scope."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    consumer_scope_id: str = Field(min_length=1, max_length=160)
    source_role: str = Field(min_length=1, max_length=80)
    dataset_name: str = Field(min_length=1, max_length=160)
    query_template_id: str = Field(min_length=1, max_length=160)
    query_template_version: str = Field(min_length=1, max_length=80)
    query_template_hash: str = Field(min_length=64, max_length=64)
    bound_parameters: dict[str, Any]
    bound_parameter_hash: str = Field(min_length=64, max_length=64)
    partition_key: dict[str, Any] = Field(min_length=1)
    revision_kind: SourceRevisionKind
    availability_requirement: AvailabilityRequirement
    business_min_date: date
    business_max_date: date
    requested_cutoff: datetime
    enforced_cutoff_predicate_hash: str = Field(min_length=64, max_length=64)
    required_quality_status: Literal["PASS"] = "PASS"
    research_only: bool = True
    common_pit_identity_hash: str = Field(min_length=64, max_length=64)
    requirement_id: str | None = Field(default=None, min_length=1, max_length=160)
    requirement_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "query_template_hash",
        "bound_parameter_hash",
        "enforced_cutoff_predicate_hash",
        "common_pit_identity_hash",
    )
    @classmethod
    def _sha256(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name)

    @field_validator("requested_cutoff")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="requested_cutoff")

    @property
    def partition_key_hash(self) -> str:
        return canonical_json_sha256(canonicalize(self.partition_key))

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": SOURCE_REQUIREMENT_SCHEMA_VERSION,
            "consumer_scope_id": self.consumer_scope_id,
            "source_role": self.source_role,
            "dataset_name": self.dataset_name,
            "query_template_id": self.query_template_id,
            "query_template_version": self.query_template_version,
            "query_template_hash": self.query_template_hash,
            "bound_parameters": canonicalize(self.bound_parameters),
            "bound_parameter_hash": self.bound_parameter_hash,
            "partition_key": canonicalize(self.partition_key),
            "partition_key_hash": self.partition_key_hash,
            "revision_kind": self.revision_kind.value,
            "availability_requirement": self.availability_requirement.value,
            "business_min_date": self.business_min_date,
            "business_max_date": self.business_max_date,
            "requested_cutoff": self.requested_cutoff,
            "enforced_cutoff_predicate_hash": self.enforced_cutoff_predicate_hash,
            "required_quality_status": self.required_quality_status,
            "research_only": self.research_only,
            "common_pit_identity_hash": self.common_pit_identity_hash,
        }

    @model_validator(mode="after")
    def _derive_identity(self) -> "SourceRequirement":
        if self.business_min_date > self.business_max_date:
            raise ValueError(REASON_SOURCE_REQUIREMENT_INVALID)
        if not self.research_only:
            raise ValueError("source requirements must remain research-only")
        if canonical_json_sha256(canonicalize(self.bound_parameters)) != self.bound_parameter_hash:
            raise ValueError("bound_parameter_hash does not match bound_parameters")
        digest = canonical_json_sha256(self.canonical_payload())
        requirement_id = f"srq_{digest[:20]}"
        if self.requirement_hash is not None and self.requirement_hash != digest:
            raise ValueError("requirement_hash does not match source requirement")
        if self.requirement_id is not None and self.requirement_id != requirement_id:
            raise ValueError("requirement_id does not match source requirement")
        object.__setattr__(self, "requirement_hash", digest)
        object.__setattr__(self, "requirement_id", requirement_id)
        return self


class SourceRequirementSet(BaseModel):
    """Frozen common PIT identity and its source requirements."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    admission_scope_id: str = Field(min_length=1, max_length=160)
    admission_scope_hash: str = Field(min_length=64, max_length=64)
    handoff_readiness_hash: str = Field(min_length=64, max_length=64)
    program_id: str = Field(min_length=1, max_length=160)
    binding_version_id: str = Field(min_length=1, max_length=160)
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: Literal["single_alpha", "multi_alpha"]
    decision_as_of_trade_date: date
    requested_source_cutoff: datetime
    label_as_of_ts: datetime
    query_registry_hash: str = Field(min_length=64, max_length=64)
    calendar_hash: str = Field(min_length=64, max_length=64)
    universe_policy_hash: str = Field(min_length=64, max_length=64)
    formal_oos_status: Literal["RETROSPECTIVE_RESEARCH_ONLY", "NONE"]
    evidence_scope: Literal["RETROSPECTIVE_RESEARCH_ONLY", "GAP_ONLY"]
    research_replay_eligible: bool = False
    data_source: Literal["DB_HISTORICAL"] = "DB_HISTORICAL"
    execution_origin: Literal["MANUAL_HISTORICAL_RESEARCH"] = "MANUAL_HISTORICAL_RESEARCH"
    research_scope: Literal["HISTORICAL_RESEARCH_ONLY"] = "HISTORICAL_RESEARCH_ONLY"
    execution_prohibited: bool = True
    research_only: bool = True
    requirements: tuple[SourceRequirement, ...] = Field(min_length=1)
    common_pit_identity_hash: str | None = Field(default=None, min_length=64, max_length=64)
    source_requirement_set_id: str | None = Field(default=None, min_length=1, max_length=160)
    source_requirement_set_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "admission_scope_hash",
        "handoff_readiness_hash",
        "manifest_sha256",
        "query_registry_hash",
        "calendar_hash",
        "universe_policy_hash",
        "common_pit_identity_hash",
    )
    @classmethod
    def _sha256(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name)

    @field_validator("requested_source_cutoff", "label_as_of_ts")
    @classmethod
    def _aware(cls, value: datetime, info) -> datetime:  # type: ignore[no-untyped-def]
        return _require_aware(value, field_name=info.field_name)

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": SOURCE_REQUIREMENT_SET_SCHEMA_VERSION,
            "admission_scope_id": self.admission_scope_id,
            "admission_scope_hash": self.admission_scope_hash,
            "handoff_readiness_hash": self.handoff_readiness_hash,
            "program_id": self.program_id,
            "binding_version_id": self.binding_version_id,
            "package_id": self.package_id,
            "manifest_sha256": self.manifest_sha256,
            "alpha_mode": self.alpha_mode,
            "decision_as_of_trade_date": self.decision_as_of_trade_date,
            "requested_source_cutoff": self.requested_source_cutoff,
            "label_as_of_ts": self.label_as_of_ts,
            "query_registry_hash": self.query_registry_hash,
            "calendar_hash": self.calendar_hash,
            "universe_policy_hash": self.universe_policy_hash,
            "formal_oos_status": self.formal_oos_status,
            "evidence_scope": self.evidence_scope,
            "research_replay_eligible": self.research_replay_eligible,
            "data_source": self.data_source,
            "execution_origin": self.execution_origin,
            "research_scope": self.research_scope,
            "execution_prohibited": self.execution_prohibited,
            "research_only": self.research_only,
            "common_pit_identity_hash": self.expected_common_pit_identity_hash,
            "requirements": [
                requirement.canonical_payload()
                for requirement in sorted(self.requirements, key=lambda item: str(item.requirement_id))
            ],
        }

    def effective_cutoff_for(self, requirement: SourceRequirement) -> datetime:
        if requirement.availability_requirement is AvailabilityRequirement.LABEL_AS_OF:
            return self.label_as_of_ts
        return self.requested_source_cutoff

    @property
    def expected_common_pit_identity_hash(self) -> str:
        return build_source_requirement_common_pit_identity_hash(
            admission_scope_id=self.admission_scope_id,
            admission_scope_hash=self.admission_scope_hash,
            handoff_readiness_hash=self.handoff_readiness_hash,
            program_id=self.program_id,
            binding_version_id=self.binding_version_id,
            package_id=self.package_id,
            manifest_sha256=self.manifest_sha256,
            alpha_mode=self.alpha_mode,
            decision_as_of_trade_date=self.decision_as_of_trade_date,
            requested_source_cutoff=self.requested_source_cutoff,
            query_registry_hash=self.query_registry_hash,
            calendar_hash=self.calendar_hash,
            universe_policy_hash=self.universe_policy_hash,
            data_source=self.data_source,
            execution_origin=self.execution_origin,
            research_scope=self.research_scope,
            execution_prohibited=self.execution_prohibited,
            research_only=self.research_only,
        )

    @model_validator(mode="after")
    def _derive_identity(self) -> "SourceRequirementSet":
        if self.label_as_of_ts < self.requested_source_cutoff:
            raise ValueError("label_as_of_ts cannot precede requested_source_cutoff")
        if not self.research_only or self.execution_prohibited is not True:
            raise ValueError("source requirement set must remain execution-prohibited research")
        if self.formal_oos_status == "RETROSPECTIVE_RESEARCH_ONLY" and self.evidence_scope != "RETROSPECTIVE_RESEARCH_ONLY":
            raise ValueError("retrospective scope requires RETROSPECTIVE_RESEARCH_ONLY evidence scope")
        if self.formal_oos_status == "NONE" and self.evidence_scope != "GAP_ONLY":
            raise ValueError("NONE formal OOS status requires GAP_ONLY evidence scope")
        requirement_ids = [str(requirement.requirement_id) for requirement in self.requirements]
        if len(requirement_ids) != len(set(requirement_ids)):
            raise ValueError(REASON_SOURCE_REQUIREMENT_CONFLICT)
        expected_common_pit_identity_hash = self.expected_common_pit_identity_hash
        if self.common_pit_identity_hash is not None and self.common_pit_identity_hash != expected_common_pit_identity_hash:
            raise ValueError("common_pit_identity_hash does not match source requirement set")
        if any(requirement.common_pit_identity_hash != expected_common_pit_identity_hash for requirement in self.requirements):
            raise ValueError(REASON_SOURCE_REQUIREMENT_CONFLICT)
        if any(requirement.requested_cutoff != self.effective_cutoff_for(requirement) for requirement in self.requirements):
            raise ValueError("source requirement does not use its frozen availability cutoff")
        digest = canonical_json_sha256(self.canonical_payload())
        requirement_set_id = f"srqs_{digest[:20]}"
        if self.source_requirement_set_hash is not None and self.source_requirement_set_hash != digest:
            raise ValueError("source_requirement_set_hash does not match source requirement set")
        if self.source_requirement_set_id is not None and self.source_requirement_set_id != requirement_set_id:
            raise ValueError("source_requirement_set_id does not match source requirement set")
        object.__setattr__(self, "source_requirement_set_hash", digest)
        object.__setattr__(self, "source_requirement_set_id", requirement_set_id)
        object.__setattr__(self, "common_pit_identity_hash", expected_common_pit_identity_hash)
        return self


class RequirementResolution(BaseModel):
    """The one immutable source-resolution outcome for one requirement."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    requirement_id: str = Field(min_length=1, max_length=160)
    consumer_scope_id: str = Field(min_length=1, max_length=160)
    resolution_status: RequirementResolutionStatus
    selected_availability_event_hash: str | None = Field(default=None, min_length=64, max_length=64)
    selected_source_member_key: str | None = Field(default=None, min_length=64, max_length=64)
    reason_codes: tuple[str, ...] = ()
    resolution_content_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("selected_availability_event_hash", "selected_source_member_key", "resolution_content_hash")
    @classmethod
    def _sha256(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name) if value is not None else None

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "requirement_id": self.requirement_id,
            "consumer_scope_id": self.consumer_scope_id,
            "resolution_status": self.resolution_status.value,
            "selected_availability_event_hash": self.selected_availability_event_hash,
            "selected_source_member_key": self.selected_source_member_key,
            "reason_codes": list(_normalized_reason_codes(self.reason_codes)),
        }

    @model_validator(mode="after")
    def _derive_hash(self) -> "RequirementResolution":
        has_selection = self.selected_availability_event_hash is not None or self.selected_source_member_key is not None
        if self.resolution_status is RequirementResolutionStatus.AVAILABLE:
            if self.selected_availability_event_hash is None or self.selected_source_member_key is None or self.reason_codes:
                raise ValueError("AVAILABLE source resolution requires exact event/member and no reason code")
        elif has_selection or not self.reason_codes:
            raise ValueError("unavailable/conflict source resolution requires reasons and no selected member")
        digest = canonical_json_sha256(self.canonical_payload())
        if self.resolution_content_hash is not None and self.resolution_content_hash != digest:
            raise ValueError("resolution_content_hash does not match resolution")
        object.__setattr__(self, "reason_codes", _normalized_reason_codes(self.reason_codes))
        object.__setattr__(self, "resolution_content_hash", digest)
        return self


class SourceResolutionReceipt(BaseModel):
    """Immutable resolution receipt, including gaps and exact source set identity."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_requirement_set_id: str = Field(min_length=1, max_length=160)
    source_requirement_set_hash: str = Field(min_length=64, max_length=64)
    requested_source_cutoff: datetime
    resolution_policy_version: str = SOURCE_RESOLUTION_POLICY_VERSION
    resolution_policy_hash: str = SOURCE_RESOLUTION_POLICY_HASH
    source_revision_set_id: str | None = Field(default=None, min_length=1, max_length=160)
    source_revision_set_hash: str | None = Field(default=None, min_length=64, max_length=64)
    requirement_resolutions: tuple[RequirementResolution, ...]
    readiness: ResearchReadiness
    reason_codes: tuple[str, ...] = ()
    research_only: bool = True
    source_resolution_receipt_id: str | None = Field(default=None, min_length=1, max_length=160)
    source_resolution_receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "source_requirement_set_hash",
        "resolution_policy_hash",
        "source_revision_set_hash",
        "source_resolution_receipt_hash",
    )
    @classmethod
    def _sha256(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("requested_source_cutoff")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="requested_source_cutoff")

    @property
    def resolved_requirement_count(self) -> int:
        return sum(item.resolution_status is RequirementResolutionStatus.AVAILABLE for item in self.requirement_resolutions)

    @property
    def unavailable_requirement_count(self) -> int:
        return sum(item.resolution_status is RequirementResolutionStatus.UNAVAILABLE for item in self.requirement_resolutions)

    @property
    def conflict_requirement_count(self) -> int:
        return sum(item.resolution_status is RequirementResolutionStatus.CONFLICT for item in self.requirement_resolutions)

    @property
    def can_create_capture_plan(self) -> bool:
        return self.readiness in {ResearchReadiness.RESEARCH_READY, ResearchReadiness.PARTIAL} and self.source_revision_set_hash is not None

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": SOURCE_RESOLUTION_RECEIPT_SCHEMA_VERSION,
            "source_requirement_set_id": self.source_requirement_set_id,
            "source_requirement_set_hash": self.source_requirement_set_hash,
            "requested_source_cutoff": self.requested_source_cutoff,
            "resolution_policy_version": self.resolution_policy_version,
            "resolution_policy_hash": self.resolution_policy_hash,
            "source_revision_set_id": self.source_revision_set_id,
            "source_revision_set_hash": self.source_revision_set_hash,
            "requirement_resolutions": [
                item.canonical_payload()
                for item in sorted(self.requirement_resolutions, key=lambda value: value.requirement_id)
            ],
            "readiness": self.readiness.value,
            "reason_codes": list(_normalized_reason_codes(self.reason_codes)),
            "research_only": self.research_only,
        }

    @model_validator(mode="after")
    def _derive_identity(self) -> "SourceResolutionReceipt":
        if not self.research_only:
            raise ValueError("source resolution receipt must remain research-only")
        if self.resolution_policy_version != SOURCE_RESOLUTION_POLICY_VERSION or self.resolution_policy_hash != SOURCE_RESOLUTION_POLICY_HASH:
            raise ValueError("source resolution policy identity is invalid")
        ids = [item.requirement_id for item in self.requirement_resolutions]
        if not ids or len(ids) != len(set(ids)):
            raise ValueError(REASON_SOURCE_RESOLUTION_CONFLICT)
        if (self.source_revision_set_id is None) != (self.source_revision_set_hash is None):
            raise ValueError("source revision set id and hash must be provided together")
        if self.readiness is ResearchReadiness.RESEARCH_READY and (
            self.unavailable_requirement_count != 0
            or self.conflict_requirement_count != 0
            or self.source_revision_set_hash is None
        ):
            raise ValueError("RESEARCH_READY receipt requires complete exact source resolution")
        if self.readiness is ResearchReadiness.PARTIAL and self.conflict_requirement_count != 0:
            raise ValueError("PARTIAL receipt cannot contain source conflicts")
        if self.readiness is ResearchReadiness.BLOCKED and not self.reason_codes:
            raise ValueError("BLOCKED receipt requires an explicit blocking reason")
        if self.readiness is ResearchReadiness.BLOCKED and self.source_revision_set_hash is not None:
            raise ValueError("BLOCKED receipt cannot expose a source revision set")
        digest = canonical_json_sha256(self.canonical_payload())
        receipt_id = f"srr_{digest[:20]}"
        if self.source_resolution_receipt_hash is not None and self.source_resolution_receipt_hash != digest:
            raise ValueError("source_resolution_receipt_hash does not match receipt")
        if self.source_resolution_receipt_id is not None and self.source_resolution_receipt_id != receipt_id:
            raise ValueError("source_resolution_receipt_id does not match receipt")
        object.__setattr__(self, "reason_codes", _normalized_reason_codes(self.reason_codes))
        object.__setattr__(self, "source_resolution_receipt_hash", digest)
        object.__setattr__(self, "source_resolution_receipt_id", receipt_id)
        return self


@dataclass(frozen=True)
class SourceResolutionResult:
    receipt: SourceResolutionReceipt
    source_revision_set: SourceRevisionSet | None

    @property
    def can_create_capture_plan(self) -> bool:
        return self.receipt.can_create_capture_plan and self.source_revision_set is not None


class InMemorySourceRequirementSetRepository:
    """Immutable local oracle for requirement-set retry and conflict semantics."""

    def __init__(self) -> None:
        self._by_id: dict[str, SourceRequirementSet] = {}
        self._by_hash: dict[str, SourceRequirementSet] = {}

    def save(self, requirement_set: SourceRequirementSet) -> SourceRequirementSet:
        identity = str(requirement_set.source_requirement_set_id)
        content_hash = str(requirement_set.source_requirement_set_hash)
        existing_by_id = self._by_id.get(identity)
        existing_by_hash = self._by_hash.get(content_hash)
        if existing_by_id is not None and existing_by_id.source_requirement_set_hash != content_hash:
            raise SourceLedgerError(REASON_SOURCE_REQUIREMENT_SET_CONFLICT, "requirement set id already binds different content")
        if existing_by_hash is not None and existing_by_hash.source_requirement_set_id != identity:
            raise SourceLedgerError(REASON_SOURCE_REQUIREMENT_SET_CONFLICT, "requirement set hash already binds different identity")
        if existing_by_id is not None:
            return existing_by_id
        self._by_id[identity] = requirement_set
        self._by_hash[content_hash] = requirement_set
        return requirement_set

    def get(self, requirement_set_id: str) -> SourceRequirementSet | None:
        return self._by_id.get(requirement_set_id)


class InMemorySourceResolutionReceiptRepository:
    """Immutable local oracle for resolution-receipt retry and conflict semantics."""

    def __init__(self) -> None:
        self._by_id: dict[str, SourceResolutionReceipt] = {}
        self._by_hash: dict[str, SourceResolutionReceipt] = {}

    def save(self, receipt: SourceResolutionReceipt) -> SourceResolutionReceipt:
        identity = str(receipt.source_resolution_receipt_id)
        content_hash = str(receipt.source_resolution_receipt_hash)
        existing_by_id = self._by_id.get(identity)
        existing_by_hash = self._by_hash.get(content_hash)
        if existing_by_id is not None and existing_by_id.source_resolution_receipt_hash != content_hash:
            raise SourceLedgerError(REASON_SOURCE_RESOLUTION_RECEIPT_CONFLICT, "resolution receipt id already binds different content")
        if existing_by_hash is not None and existing_by_hash.source_resolution_receipt_id != identity:
            raise SourceLedgerError(REASON_SOURCE_RESOLUTION_RECEIPT_CONFLICT, "resolution receipt hash already binds different identity")
        if existing_by_id is not None:
            return existing_by_id
        self._by_id[identity] = receipt
        self._by_hash[content_hash] = receipt
        return receipt

    def get(self, receipt_id: str) -> SourceResolutionReceipt | None:
        return self._by_id.get(receipt_id)


class FixtureSourceRevisionResolver:
    """Resolve explicit fixture events without mutable source or runtime dependencies."""

    def resolve(
        self,
        *,
        requirement_set: SourceRequirementSet,
        availability_events: Iterable[SourceAvailabilityEvent],
    ) -> SourceResolutionResult:
        events_by_chain: dict[str, list[SourceAvailabilityEvent]] = {}
        for event in availability_events:
            events_by_chain.setdefault(event.partition_chain_key, []).append(event)

        resolutions: list[RequirementResolution] = []
        members_by_requirement_id: dict[str, SourceRevisionMemberInput] = {}
        for requirement in sorted(requirement_set.requirements, key=lambda item: str(item.requirement_id)):
            resolution, member = self._resolve_requirement(
                requirement=requirement,
                effective_cutoff=requirement_set.effective_cutoff_for(requirement),
                events=events_by_chain.get(self._chain_key(requirement), []),
            )
            resolutions.append(resolution)
            if member is not None:
                members_by_requirement_id[str(requirement.requirement_id)] = member

        resolutions = self._resolve_member_key_conflicts(
            resolutions=resolutions,
            members_by_requirement_id=members_by_requirement_id,
        )
        readiness = self._classify_readiness(requirement_set=requirement_set, resolutions=resolutions)
        available_member_keys = {
            resolution.selected_source_member_key
            for resolution in resolutions
            if resolution.resolution_status is RequirementResolutionStatus.AVAILABLE
        }
        ordered_members = [
            member
            for requirement_id, member in sorted(members_by_requirement_id.items())
            if member.member_key in available_member_keys
        ]
        unique_members = self._unique_members(ordered_members)
        source_revision_set: SourceRevisionSet | None = None
        if readiness is not ResearchReadiness.BLOCKED and unique_members:
            try:
                source_revision_set = build_source_revision_set(
                    query_registry_hash=requirement_set.query_registry_hash,
                    requested_source_cutoff=requirement_set.requested_source_cutoff,
                    label_as_of_ts=requirement_set.label_as_of_ts,
                    research_only=True,
                    members=unique_members,
                )
            except SourceLedgerError as error:
                resolutions = self._mark_available_as_conflict(resolutions, reason_code=error.reason_code)
                source_revision_set = None
                readiness = self._classify_readiness(requirement_set=requirement_set, resolutions=resolutions)

        reasons = list(_normalized_reason_codes(reason for resolution in resolutions for reason in resolution.reason_codes))
        if requirement_set.formal_oos_status == "NONE" and not requirement_set.research_replay_eligible:
            reasons.append(REASON_SOURCE_REPLAY_NOT_ELIGIBLE)
        elif readiness is ResearchReadiness.BLOCKED and not reasons:
            reasons.append(REASON_SOURCE_RESOLUTION_CONFLICT)
        receipt = SourceResolutionReceipt(
            source_requirement_set_id=str(requirement_set.source_requirement_set_id),
            source_requirement_set_hash=str(requirement_set.source_requirement_set_hash),
            requested_source_cutoff=requirement_set.requested_source_cutoff,
            source_revision_set_id=source_revision_set.source_revision_set_id if source_revision_set else None,
            source_revision_set_hash=source_revision_set.source_revision_set_hash if source_revision_set else None,
            requirement_resolutions=tuple(sorted(resolutions, key=lambda item: item.requirement_id)),
            readiness=readiness,
            reason_codes=tuple(reasons),
            research_only=True,
        )
        return SourceResolutionResult(receipt=receipt, source_revision_set=source_revision_set)

    @staticmethod
    def _chain_key(requirement: SourceRequirement) -> str:
        return source_partition_chain_key(
            dataset_name=requirement.dataset_name,
            source_role=requirement.source_role,
            partition_key=requirement.partition_key,
        )

    def _resolve_requirement(
        self,
        *,
        requirement: SourceRequirement,
        effective_cutoff: datetime,
        events: list[SourceAvailabilityEvent],
    ) -> tuple[RequirementResolution, SourceRevisionMemberInput | None]:
        if not events:
            return self._unavailable(requirement, REASON_SOURCE_UNAVAILABLE_AS_OF), None
        try:
            ordered_events = self._validate_event_chain(requirement=requirement, events=events)
        except SourceLedgerError:
            return self._conflict(requirement, REASON_SOURCE_CHAIN_INVALID), None
        candidates = [event for event in ordered_events if event.formal_available_at <= effective_cutoff]
        if not candidates:
            return self._unavailable(requirement, REASON_SOURCE_UNAVAILABLE_AS_OF), None
        terminal = candidates[-1]
        if terminal.event_type is SourceAvailabilityEventType.INVALIDATED:
            return self._unavailable(requirement, REASON_SOURCE_TERMINAL_INVALIDATED), None
        if terminal.input.quality_status != requirement.required_quality_status:
            return self._unavailable(requirement, REASON_SOURCE_QUALITY_INVALID), None
        try:
            member = SourceRevisionMemberInput(
                source_role=requirement.source_role,
                dataset_name=requirement.dataset_name,
                query_template_id=requirement.query_template_id,
                query_template_version=requirement.query_template_version,
                query_template_hash=requirement.query_template_hash,
                bound_parameter_hash=requirement.bound_parameter_hash,
                enforced_cutoff_predicate_hash=requirement.enforced_cutoff_predicate_hash,
                partition_key=requirement.partition_key,
                revision_kind=requirement.revision_kind,
                revision_id=terminal.input.revision_id,
                availability_requirement=requirement.availability_requirement,
                business_min_date=requirement.business_min_date,
                business_max_date=requirement.business_max_date,
                available_at_min=terminal.formal_available_at,
                available_at_max=terminal.formal_available_at,
                schema_fingerprint=terminal.input.schema_fingerprint,
                row_count=terminal.input.row_count,
                partition_content_hash=terminal.input.partition_content_hash,
                quality_status=terminal.input.quality_status,
                availability_event=terminal,
                research_only=True,
            )
        except ValueError:
            return self._conflict(requirement, REASON_SOURCE_MEMBER_MISMATCH), None
        return (
            RequirementResolution(
                requirement_id=str(requirement.requirement_id),
                consumer_scope_id=requirement.consumer_scope_id,
                resolution_status=RequirementResolutionStatus.AVAILABLE,
                selected_availability_event_hash=terminal.event_content_hash,
                selected_source_member_key=member.member_key,
            ),
            member,
        )

    def _validate_event_chain(
        self,
        *,
        requirement: SourceRequirement,
        events: list[SourceAvailabilityEvent],
    ) -> list[SourceAvailabilityEvent]:
        expected_chain_key = self._chain_key(requirement)
        ordered = validate_source_availability_event_chain(
            events,
            expected_partition_chain_key=expected_chain_key,
        )
        if any(
            event.input.dataset_name != requirement.dataset_name
            or event.input.source_role != requirement.source_role
            or event.input.partition_key_hash != requirement.partition_key_hash
            for event in ordered
        ):
            raise SourceLedgerError(REASON_EVENT_CHAIN_INVALID, "fixture source event identity differs from requirement")
        return list(ordered)

    @staticmethod
    def _unavailable(requirement: SourceRequirement, reason_code: str) -> RequirementResolution:
        return RequirementResolution(
            requirement_id=str(requirement.requirement_id),
            consumer_scope_id=requirement.consumer_scope_id,
            resolution_status=RequirementResolutionStatus.UNAVAILABLE,
            reason_codes=(reason_code,),
        )

    @staticmethod
    def _conflict(requirement: SourceRequirement, reason_code: str) -> RequirementResolution:
        return RequirementResolution(
            requirement_id=str(requirement.requirement_id),
            consumer_scope_id=requirement.consumer_scope_id,
            resolution_status=RequirementResolutionStatus.CONFLICT,
            reason_codes=(reason_code,),
        )

    @staticmethod
    def _unique_members(members: Iterable[SourceRevisionMemberInput]) -> list[SourceRevisionMemberInput]:
        by_key: dict[str, SourceRevisionMemberInput] = {}
        for member in members:
            by_key.setdefault(member.member_key, member)
        return [by_key[key] for key in sorted(by_key)]

    def _resolve_member_key_conflicts(
        self,
        *,
        resolutions: list[RequirementResolution],
        members_by_requirement_id: dict[str, SourceRevisionMemberInput],
    ) -> list[RequirementResolution]:
        members_by_key: dict[str, list[tuple[str, SourceRevisionMemberInput]]] = {}
        for requirement_id, member in members_by_requirement_id.items():
            members_by_key.setdefault(member.member_key, []).append((requirement_id, member))
        conflicting_ids: set[str] = set()
        for members in members_by_key.values():
            payloads = {canonical_json_sha256(member.content_payload()) for _, member in members}
            if len(payloads) > 1:
                conflicting_ids.update(requirement_id for requirement_id, _ in members)
        if not conflicting_ids:
            return resolutions
        for requirement_id in conflicting_ids:
            members_by_requirement_id.pop(requirement_id, None)
        return [
            RequirementResolution(
                requirement_id=item.requirement_id,
                consumer_scope_id=item.consumer_scope_id,
                resolution_status=RequirementResolutionStatus.CONFLICT,
                reason_codes=(REASON_SOURCE_RESOLUTION_CONFLICT,),
            )
            if item.requirement_id in conflicting_ids
            else item
            for item in resolutions
        ]

    @staticmethod
    def _mark_available_as_conflict(
        resolutions: list[RequirementResolution],
        *,
        reason_code: str,
    ) -> list[RequirementResolution]:
        return [
            RequirementResolution(
                requirement_id=item.requirement_id,
                consumer_scope_id=item.consumer_scope_id,
                resolution_status=RequirementResolutionStatus.CONFLICT,
                reason_codes=(reason_code,),
            )
            if item.resolution_status is RequirementResolutionStatus.AVAILABLE
            else item
            for item in resolutions
        ]

    @staticmethod
    def _classify_readiness(
        *,
        requirement_set: SourceRequirementSet,
        resolutions: Iterable[RequirementResolution],
    ) -> ResearchReadiness:
        items = tuple(resolutions)
        if any(item.resolution_status is RequirementResolutionStatus.CONFLICT for item in items):
            return ResearchReadiness.BLOCKED
        if requirement_set.formal_oos_status == "RETROSPECTIVE_RESEARCH_ONLY":
            return (
                ResearchReadiness.RESEARCH_READY
                if all(item.resolution_status is RequirementResolutionStatus.AVAILABLE for item in items)
                else ResearchReadiness.PARTIAL
            )
        if requirement_set.formal_oos_status == "NONE" and requirement_set.research_replay_eligible:
            return ResearchReadiness.PARTIAL
        return ResearchReadiness.BLOCKED
