"""Pure Batch B contracts for historical-research label capture.

This module deliberately contains no database access, current-control lookup,
runtime registration, or model training entry point.  It turns already frozen
Phase 1C-1/1C-2 evidence into a label-capture request that can be executed by
the existing in-memory capture state machine.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.label_policy import LabelPolicyBundle
from backend.services.advisory_phase1.observation_selector import (
    FixtureObservationVersion,
    ObservationSelectionStatus,
    SelectedObservationMapping,
)
from backend.services.advisory_phase1.source_ledger import SourceLedgerError
from backend.services.advisory_phase1.source_revision import SourceRevisionSet, build_source_revision_set


LABEL_CAPTURE_BINDING_SCHEMA_VERSION = "advisory_phase1_label_capture_binding_v1"
LABEL_CAPTURE_BATCH_SCHEMA_VERSION = "advisory_phase1_capture_batch_v2"
LABEL_CAPTURE_PURPOSE = "LABEL_CAPTURE_V1"

REASON_LABEL_CAPTURE_BINDING_INVALID = "ADVISORY_PHASE1C3_LABEL_CAPTURE_BINDING_INVALID"
REASON_LABEL_CAPTURE_SOURCE_BATCH_INVALID = "ADVISORY_PHASE1C3_LABEL_CAPTURE_SOURCE_BATCH_INVALID"
REASON_LABEL_CAPTURE_MAPPING_SET_INVALID = "ADVISORY_PHASE1C3_LABEL_CAPTURE_MAPPING_SET_INVALID"
REASON_LABEL_CAPTURE_PLAN_SET_INVALID = "ADVISORY_PHASE1C3_LABEL_CAPTURE_PLAN_SET_INVALID"
REASON_LABEL_CAPTURE_PLANNED_SET_INVALID = "ADVISORY_PHASE1C3_LABEL_CAPTURE_PLANNED_SET_INVALID"
REASON_LABEL_CAPTURE_OUTPUT_CLOSURE_INVALID = "ADVISORY_PHASE1C3_LABEL_CAPTURE_OUTPUT_CLOSURE_INVALID"


class LabelCaptureContractError(SourceLedgerError):
    """Stable failure for an immutable label-capture contract violation."""


def _require_sha256(value: str, *, field_name: str) -> str:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{field_name} must be lowercase sha256 hex")
    return value


def _require_aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include an explicit timezone")
    return value.astimezone(timezone.utc)


def _membership_set_hash(values: Iterable["CaptureEvidenceMembershipReference"]) -> tuple[int, str]:
    ordered = [item.model_dump(mode="json") for item in sorted(values, key=lambda item: item.content_key)]
    return len(ordered), canonical_json_sha256(ordered)


class CaptureEvidenceMembershipReference(BaseModel):
    """Typed copy of a frozen capture membership, independent of its repository."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    evidence_role: str = Field(min_length=1, max_length=80)
    evidence_id: str = Field(min_length=1, max_length=160)
    evidence_content_hash: str = Field(min_length=64, max_length=64)

    @field_validator("evidence_content_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _require_sha256(value, field_name="evidence_content_hash")

    @property
    def content_key(self) -> tuple[str, str]:
        return self.evidence_role, self.evidence_id

    def canonical_identity(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class CapturePlanReference(BaseModel):
    """The existing v1 CapturePlan identity required by one label admission."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    selection_run_id: str = Field(min_length=1, max_length=160)
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    plan_hash: str = Field(min_length=64, max_length=64)

    @field_validator("manifest_sha256", "plan_hash")
    @classmethod
    def _hash(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name)

    def canonical_identity(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class SelectedObservationMappingReference(BaseModel):
    """Frozen selected-observation mapping identity and its terminal evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    selected_mapping_id: str = Field(min_length=1, max_length=160)
    selected_mapping_hash: str = Field(min_length=64, max_length=64)
    canonical_signal_id: str = Field(min_length=1, max_length=160)
    terminal_observation_version_id: str = Field(min_length=1, max_length=160)
    terminal_observation_content_hash: str = Field(min_length=64, max_length=64)
    terminal_revision_no: int = Field(ge=1)

    @field_validator("selected_mapping_hash", "terminal_observation_content_hash")
    @classmethod
    def _hash(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name)

    @classmethod
    def from_mapping(cls, mapping: SelectedObservationMapping) -> "SelectedObservationMappingReference":
        if (
            mapping.selection_status is not ObservationSelectionStatus.SELECTED
            or mapping.selected_mapping_id is None
            or mapping.selected_mapping_hash is None
            or mapping.terminal_observation_version_id is None
            or mapping.terminal_observation_content_hash is None
            or mapping.terminal_revision_no is None
        ):
            raise LabelCaptureContractError(
                REASON_LABEL_CAPTURE_MAPPING_SET_INVALID,
                "label capture requires a selected observation mapping with one terminal version",
            )
        return cls(
            selected_mapping_id=mapping.selected_mapping_id,
            selected_mapping_hash=mapping.selected_mapping_hash,
            canonical_signal_id=mapping.canonical_signal_id,
            terminal_observation_version_id=mapping.terminal_observation_version_id,
            terminal_observation_content_hash=mapping.terminal_observation_content_hash,
            terminal_revision_no=mapping.terminal_revision_no,
        )

    def canonical_identity(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class LabelCaptureBinding(BaseModel):
    """Historical provenance binding for one new label-capture batch."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = LABEL_CAPTURE_BINDING_SCHEMA_VERSION
    capture_batch_id: str = Field(min_length=1, max_length=160)
    current_fencing_token: int = Field(ge=1)
    source_observation_capture_batch_id: str = Field(min_length=1, max_length=160)
    source_capture_request_hash: str = Field(min_length=64, max_length=64)
    source_capture_receipt_hash: str = Field(min_length=64, max_length=64)
    source_capture_membership_count: int = Field(ge=0)
    source_capture_membership_hash: str = Field(min_length=64, max_length=64)
    source_capture_plan_set_count: int = Field(ge=1)
    source_capture_plan_set_hash: str = Field(min_length=64, max_length=64)
    source_trace_binding_hash: str = Field(min_length=64, max_length=64)
    source_control_binding_event_hash: str = Field(min_length=64, max_length=64)
    phase1_handoff_bundle_hash: str = Field(min_length=64, max_length=64)
    handoff_readiness_hash: str = Field(min_length=64, max_length=64)
    admission_scope_id: str = Field(min_length=1, max_length=160)
    admission_scope_hash: str = Field(min_length=64, max_length=64)
    selected_observation_mapping_set_count: int = Field(ge=1)
    selected_observation_mapping_set_hash: str = Field(min_length=64, max_length=64)
    label_policy_bundle_id: str = Field(min_length=1, max_length=160)
    label_policy_bundle_hash: str = Field(min_length=64, max_length=64)
    label_source_revision_set_id: str = Field(min_length=1, max_length=160)
    label_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    label_as_of_ts: datetime
    binding_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "source_capture_request_hash",
        "source_capture_receipt_hash",
        "source_capture_membership_hash",
        "source_capture_plan_set_hash",
        "source_trace_binding_hash",
        "source_control_binding_event_hash",
        "phase1_handoff_bundle_hash",
        "handoff_readiness_hash",
        "admission_scope_hash",
        "selected_observation_mapping_set_hash",
        "label_policy_bundle_hash",
        "label_source_revision_set_hash",
        "binding_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("label_as_of_ts")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="label_as_of_ts")

    def canonical_payload(self) -> dict[str, Any]:
        return canonicalize(self.model_dump(mode="python", exclude={"binding_hash"}))

    @model_validator(mode="after")
    def _derive_hash(self) -> "LabelCaptureBinding":
        if self.schema_version != LABEL_CAPTURE_BINDING_SCHEMA_VERSION:
            raise ValueError("unsupported label capture binding schema version")
        digest = canonical_json_sha256(self.canonical_payload())
        if self.binding_hash is not None and self.binding_hash != digest:
            raise ValueError("binding_hash does not match label capture binding")
        object.__setattr__(self, "binding_hash", digest)
        return self


class PlannedLabelDescriptor(BaseModel):
    """One candidate label key produced from alpha_raw authority."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    owner_type: str = "CANDIDATE"
    canonical_signal_id: str = Field(min_length=1, max_length=160)
    observation_version_id: str = Field(min_length=1, max_length=160)
    candidate_stage_evidence_id: str = Field(min_length=1, max_length=160)
    symbol: str = Field(min_length=1, max_length=32)
    decision_as_of_trade_date: date
    horizon_trading_days: int = Field(ge=0)
    projection: str = Field(min_length=1, max_length=80)
    label_key_hash: str = Field(min_length=64, max_length=64)

    @field_validator("label_key_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _require_sha256(value, field_name="label_key_hash")

    @model_validator(mode="after")
    def _candidate_only(self) -> "PlannedLabelDescriptor":
        if self.owner_type != "CANDIDATE":
            raise ValueError("planned label descriptor owner_type must be CANDIDATE")
        return self

    def canonical_identity(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


def _plan_set_hash(values: Iterable[CapturePlanReference]) -> tuple[int, str]:
    ordered = [
        item.canonical_identity()
        for item in sorted(values, key=lambda item: (item.selection_run_id, item.package_id, item.manifest_sha256))
    ]
    if len({(item["selection_run_id"], item["package_id"], item["manifest_sha256"]) for item in ordered}) != len(ordered):
        raise ValueError("source capture plans contain duplicate identities")
    return len(ordered), canonical_json_sha256(ordered)


def _mapping_set_hash(values: Iterable[SelectedObservationMappingReference]) -> tuple[int, str]:
    ordered = [item.canonical_identity() for item in sorted(values, key=lambda item: item.canonical_signal_id)]
    if len({item["canonical_signal_id"] for item in ordered}) != len(ordered):
        raise ValueError("selected observation mappings contain duplicate canonical signals")
    return len(ordered), canonical_json_sha256(ordered)


def _planned_label_set_hash(values: Iterable[PlannedLabelDescriptor]) -> tuple[int, str]:
    ordered = [
        item.canonical_identity()
        for item in sorted(
            values,
            key=lambda item: (item.canonical_signal_id, item.symbol, item.horizon_trading_days, item.projection),
        )
    ]
    keys = {(item["canonical_signal_id"], item["symbol"], item["horizon_trading_days"], item["projection"]) for item in ordered}
    if len(keys) != len(ordered):
        raise ValueError("planned labels contain duplicate signal/symbol/horizon/projection identities")
    return len(ordered), canonical_json_sha256(ordered)


class CandidateCoverageSummary(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    observation_count: int = Field(ge=0)
    included_count: int = Field(ge=0)
    excluded_count: int = Field(ge=0)
    empty_observation_count: int = Field(ge=0)
    planned_label_count: int = Field(ge=0)
    maturity_counts: dict[str, int] = Field(default_factory=dict)
    outcome_event_counts: dict[str, int] = Field(default_factory=dict)
    projection_counts: dict[str, int] = Field(default_factory=dict)
    content_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("content_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return _require_sha256(value, field_name="content_hash") if value is not None else None

    @model_validator(mode="after")
    def _closure(self) -> "CandidateCoverageSummary":
        if any(value < 0 for value in (*self.maturity_counts.values(), *self.outcome_event_counts.values(), *self.projection_counts.values())):
            raise ValueError("coverage counts must be non-negative")
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"content_hash"}))
        if self.content_hash is not None and self.content_hash != digest:
            raise ValueError("candidate coverage content_hash does not match content")
        object.__setattr__(self, "content_hash", digest)
        return self


class UniverseCoverageSummary(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    frozen_constituent_count: int = Field(ge=0)
    planned_row_count: int = Field(ge=0)
    raw_row_count: int = Field(ge=0)
    maturity_counts: dict[str, int] = Field(default_factory=dict)
    outcome_event_counts: dict[str, int] = Field(default_factory=dict)
    projection_counts: dict[str, int] = Field(default_factory=dict)
    denominator_count_by_projection: dict[str, int] = Field(default_factory=dict)
    content_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("content_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return _require_sha256(value, field_name="content_hash") if value is not None else None

    @model_validator(mode="after")
    def _closure(self) -> "UniverseCoverageSummary":
        counts = (*self.maturity_counts.values(), *self.outcome_event_counts.values(), *self.projection_counts.values(), *self.denominator_count_by_projection.values())
        if any(value < 0 for value in counts):
            raise ValueError("coverage counts must be non-negative")
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"content_hash"}))
        if self.content_hash is not None and self.content_hash != digest:
            raise ValueError("universe coverage content_hash does not match content")
        object.__setattr__(self, "content_hash", digest)
        return self


class LabelCaptureGap(BaseModel):
    """Explicit evidence for one planned output that cannot be materialized."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    planned_identity: dict[str, Any]
    reason_code: str = Field(min_length=1, max_length=160)
    observed_at: datetime
    evidence_hashes: tuple[str, ...] = ()
    gap_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("observed_at")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="observed_at")

    @field_validator("evidence_hashes")
    @classmethod
    def _hashes(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        checked = tuple(_require_sha256(value, field_name="evidence_hash") for value in values)
        if tuple(sorted(set(checked))) != checked:
            raise ValueError("gap evidence hashes must be sorted and unique")
        return checked

    @field_validator("gap_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return _require_sha256(value, field_name="gap_hash") if value is not None else None

    @model_validator(mode="after")
    def _derive_hash(self) -> "LabelCaptureGap":
        payload = canonicalize(self.model_dump(mode="python", exclude={"gap_hash"}))
        digest = canonical_json_sha256(payload)
        if self.gap_hash is not None and self.gap_hash != digest:
            raise ValueError("gap_hash does not match label capture gap")
        object.__setattr__(self, "gap_hash", digest)
        return self


@dataclass(frozen=True)
class LabelCaptureAdmissionContext:
    """Explicit frozen objects used to reconstruct one label binding.

    ``source_batch`` and ``source_memberships`` intentionally retain the
    existing Phase 1C objects.  Keeping them opaque here avoids a second copy
    of the capture state machine while validation below still checks their
    canonical immutable attributes.
    """

    source_batch: Any
    source_memberships: tuple[Any, ...]
    source_plans: tuple[Any, ...]
    selected_observation_mappings: tuple[SelectedObservationMapping, ...]
    selected_observations: tuple[FixtureObservationVersion, ...]
    label_policy_bundle: LabelPolicyBundle
    label_source_revision_set: SourceRevisionSet

    @staticmethod
    def _revalidate_model(model: Any, *, reason_code: str, label: str) -> None:
        if not isinstance(model, BaseModel):
            raise LabelCaptureContractError(reason_code, f"{label} is not an immutable Pydantic contract")
        try:
            rebuilt = type(model).model_validate(model.model_dump(mode="python"))
        except (TypeError, ValueError) as error:
            raise LabelCaptureContractError(reason_code, f"{label} failed canonical revalidation") from error
        if rebuilt != model:
            raise LabelCaptureContractError(reason_code, f"{label} differs from its canonical revalidation")

    def _membership_refs(self) -> tuple[CaptureEvidenceMembershipReference, ...]:
        refs = tuple(
            CaptureEvidenceMembershipReference(
                evidence_role=str(item.evidence_role),
                evidence_id=str(item.evidence_id),
                evidence_content_hash=str(item.evidence_content_hash),
            )
            for item in self.source_memberships
        )
        if len({item.content_key for item in refs}) != len(refs):
            raise LabelCaptureContractError(REASON_LABEL_CAPTURE_SOURCE_BATCH_INVALID, "source memberships are duplicated")
        return refs

    def _plan_refs(self) -> tuple[CapturePlanReference, ...]:
        refs = tuple(
            CapturePlanReference(
                selection_run_id=str(item.selection_run_id),
                package_id=str(item.package_id),
                manifest_sha256=str(item.manifest_sha256),
                plan_hash=str(item.plan_hash),
            )
            for item in self.source_plans
        )
        if not refs:
            raise LabelCaptureContractError(REASON_LABEL_CAPTURE_PLAN_SET_INVALID, "label capture requires source capture plans")
        return refs

    def mapping_refs(self) -> tuple[SelectedObservationMappingReference, ...]:
        refs = tuple(SelectedObservationMappingReference.from_mapping(item) for item in self.selected_observation_mappings)
        if not refs:
            raise LabelCaptureContractError(REASON_LABEL_CAPTURE_MAPPING_SET_INVALID, "label capture requires selected observations")
        if len({item.selected_mapping_id for item in refs}) != len(refs):
            raise LabelCaptureContractError(REASON_LABEL_CAPTURE_MAPPING_SET_INVALID, "selected observation mappings are duplicated")
        return tuple(sorted(refs, key=lambda item: item.canonical_signal_id))

    def validate(self) -> None:
        source = self.source_batch
        request = getattr(source, "request", None)
        binding = getattr(request, "binding", None)
        if (
            source is None
            or getattr(getattr(source, "status", None), "value", getattr(source, "status", None)) != "COMPLETE"
            or request is None
            or binding is None
            or not getattr(source, "capture_receipt_hash", None)
            or not getattr(source, "membership_hash", None)
            or getattr(source, "membership_count", None) is None
        ):
            raise LabelCaptureContractError(
                REASON_LABEL_CAPTURE_SOURCE_BATCH_INVALID,
                "source observation capture batch must be complete with a sealed receipt",
            )
        self._revalidate_model(source, reason_code=REASON_LABEL_CAPTURE_SOURCE_BATCH_INVALID, label="source capture batch")
        if getattr(request, "plans", None) is None:
            raise LabelCaptureContractError(REASON_LABEL_CAPTURE_SOURCE_BATCH_INVALID, "source batch must be a v1 observation capture")
        for membership in self.source_memberships:
            self._revalidate_model(membership, reason_code=REASON_LABEL_CAPTURE_SOURCE_BATCH_INVALID, label="source membership")
        for plan in self.source_plans:
            self._revalidate_model(plan, reason_code=REASON_LABEL_CAPTURE_PLAN_SET_INVALID, label="source capture plan")
        for mapping in self.selected_observation_mappings:
            self._revalidate_model(mapping, reason_code=REASON_LABEL_CAPTURE_MAPPING_SET_INVALID, label="selected observation mapping")
        for observation in self.selected_observations:
            self._revalidate_model(observation, reason_code=REASON_LABEL_CAPTURE_MAPPING_SET_INVALID, label="selected observation")
        self._revalidate_model(self.label_policy_bundle, reason_code=REASON_LABEL_CAPTURE_BINDING_INVALID, label="label policy bundle")
        try:
            rebuilt_source_set = build_source_revision_set(
                query_registry_hash=self.label_source_revision_set.query_registry_hash,
                requested_source_cutoff=self.label_source_revision_set.requested_source_cutoff,
                label_as_of_ts=self.label_source_revision_set.label_as_of_ts,
                research_only=self.label_source_revision_set.research_only,
                members=list(self.label_source_revision_set.members),
            )
        except SourceLedgerError as error:
            raise LabelCaptureContractError(REASON_LABEL_CAPTURE_SOURCE_BATCH_INVALID, "label source revision set is invalid") from error
        if rebuilt_source_set != self.label_source_revision_set:
            raise LabelCaptureContractError(REASON_LABEL_CAPTURE_SOURCE_BATCH_INVALID, "label source revision set differs from canonical content")
        memberships = self._membership_refs()
        membership_count, membership_hash = _membership_set_hash(memberships)
        if membership_count != source.membership_count or membership_hash != source.membership_hash:
            raise LabelCaptureContractError(
                REASON_LABEL_CAPTURE_SOURCE_BATCH_INVALID,
                "provided source memberships do not reconstruct the sealed source receipt",
            )
        plan_refs = self._plan_refs()
        request_plan_refs = tuple(
            CapturePlanReference(
                selection_run_id=str(item.selection_run_id),
                package_id=str(item.package_id),
                manifest_sha256=str(item.manifest_sha256),
                plan_hash=str(item.plan_hash),
            )
            for item in request.plans
        )
        mapping_refs = self.mapping_refs()
        plan_count, plan_hash = _plan_set_hash(plan_refs)
        request_plan_count, request_plan_hash = _plan_set_hash(request_plan_refs)
        if plan_count != request_plan_count or plan_hash != request_plan_hash:
            raise LabelCaptureContractError(
                REASON_LABEL_CAPTURE_PLAN_SET_INVALID,
                "provided source plans do not match the sealed source capture request",
            )
        _mapping_set_hash(mapping_refs)
        membership_by_key = {item.content_key: item for item in memberships}
        observations_by_id = {item.observation_version_id: item for item in self.selected_observations}
        mappings_by_id = {str(item.selected_mapping_id): item for item in self.selected_observation_mappings}
        if len(observations_by_id) != len(self.selected_observations):
            raise LabelCaptureContractError(REASON_LABEL_CAPTURE_MAPPING_SET_INVALID, "selected immutable observations are duplicated")
        if len(mappings_by_id) != len(self.selected_observation_mappings):
            raise LabelCaptureContractError(REASON_LABEL_CAPTURE_MAPPING_SET_INVALID, "selected observation mappings are duplicated")
        for ref in mapping_refs:
            mapping = mappings_by_id.get(ref.selected_mapping_id)
            if mapping is None:
                raise LabelCaptureContractError(REASON_LABEL_CAPTURE_MAPPING_SET_INVALID, "mapping reference cannot be resolved")
            membership = membership_by_key.get(("selected_observation_mapping", ref.selected_mapping_id))
            if membership is None or membership.evidence_content_hash != ref.selected_mapping_hash:
                raise LabelCaptureContractError(
                    REASON_LABEL_CAPTURE_MAPPING_SET_INVALID,
                    "selected observation mapping is absent from source capture membership",
                )
            observation = observations_by_id.get(ref.terminal_observation_version_id)
            if observation is None or observation.observation_content_hash != ref.terminal_observation_content_hash:
                raise LabelCaptureContractError(
                    REASON_LABEL_CAPTURE_MAPPING_SET_INVALID,
                    "selected mapping terminal does not match the immutable observation",
                )
            if observation.canonical_signal_id != mapping.canonical_signal_id:
                raise LabelCaptureContractError(
                    REASON_LABEL_CAPTURE_MAPPING_SET_INVALID,
                    "selected observation canonical signal does not match mapping",
                )
        if not self.label_source_revision_set.research_only:
            raise LabelCaptureContractError(REASON_LABEL_CAPTURE_SOURCE_BATCH_INVALID, "label source revision set must be research only")
        if self.label_policy_bundle.label_policy_bundle_id is None or self.label_policy_bundle.label_policy_bundle_hash is None:
            raise LabelCaptureContractError(REASON_LABEL_CAPTURE_BINDING_INVALID, "label policy bundle identity is missing")
        if (
            self.label_policy_bundle.handoff_readiness_hash != binding.handoff_readiness_hash
            or self.label_policy_bundle.admission_scope_id != binding.admission_scope_id
            or self.label_policy_bundle.admission_scope_hash != binding.admission_scope_hash
        ):
            raise LabelCaptureContractError(REASON_LABEL_CAPTURE_BINDING_INVALID, "label policy does not match source capture scope")
        if any(
            plan_ref.package_id != self.label_policy_bundle.package_id
            or plan_ref.manifest_sha256 != self.label_policy_bundle.manifest_sha256
            for plan_ref in plan_refs
        ):
            raise LabelCaptureContractError(REASON_LABEL_CAPTURE_PLAN_SET_INVALID, "source plans do not match label policy package identity")

    def build_binding(self, *, capture_batch_id: str, current_fencing_token: int = 1) -> LabelCaptureBinding:
        self.validate()
        source = self.source_batch
        request = source.request
        binding = request.binding
        membership_count, membership_hash = _membership_set_hash(self._membership_refs())
        plan_count, plan_hash = _plan_set_hash(self._plan_refs())
        mapping_count, mapping_hash = _mapping_set_hash(self.mapping_refs())
        return LabelCaptureBinding(
            capture_batch_id=capture_batch_id,
            current_fencing_token=current_fencing_token,
            source_observation_capture_batch_id=str(source.request.capture_batch_id),
            source_capture_request_hash=str(request.capture_request_hash),
            source_capture_receipt_hash=str(source.capture_receipt_hash),
            source_capture_membership_count=membership_count,
            source_capture_membership_hash=membership_hash,
            source_capture_plan_set_count=plan_count,
            source_capture_plan_set_hash=plan_hash,
            source_trace_binding_hash=str(binding.binding_hash),
            source_control_binding_event_hash=str(binding.control_binding_event_hash),
            phase1_handoff_bundle_hash=str(self.label_policy_bundle.phase1_handoff_bundle_hash),
            handoff_readiness_hash=str(binding.handoff_readiness_hash),
            admission_scope_id=str(binding.admission_scope_id),
            admission_scope_hash=str(binding.admission_scope_hash),
            selected_observation_mapping_set_count=mapping_count,
            selected_observation_mapping_set_hash=mapping_hash,
            label_policy_bundle_id=str(self.label_policy_bundle.label_policy_bundle_id),
            label_policy_bundle_hash=str(self.label_policy_bundle.label_policy_bundle_hash),
            label_source_revision_set_id=str(self.label_source_revision_set.source_revision_set_id),
            label_source_revision_set_hash=str(self.label_source_revision_set.source_revision_set_hash),
            label_as_of_ts=self.label_source_revision_set.label_as_of_ts,
        )


def build_label_capture_binding(
    context: LabelCaptureAdmissionContext,
    *,
    capture_batch_id: str,
    current_fencing_token: int = 1,
) -> LabelCaptureBinding:
    """Construct the binding only from a complete immutable admission context."""

    return context.build_binding(capture_batch_id=capture_batch_id, current_fencing_token=current_fencing_token)


class LabelCaptureBatchRequestV2(BaseModel):
    """One pure, historical-only label-capture request.

    The binding's runtime batch id/fencing token and binding hash are excluded
    from the semantic request hash so an explicit recovery can retain request
    semantics while receiving a new capture batch identity.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = LABEL_CAPTURE_BATCH_SCHEMA_VERSION
    capture_purpose: str = LABEL_CAPTURE_PURPOSE
    capture_batch_id: str = Field(min_length=1, max_length=160)
    binding: LabelCaptureBinding
    source_observation_capture_batch_id: str = Field(min_length=1, max_length=160)
    source_capture_receipt_hash: str = Field(min_length=64, max_length=64)
    source_capture_membership_hash: str = Field(min_length=64, max_length=64)
    source_capture_plan_set_count: int = Field(ge=1)
    source_capture_plan_set_hash: str = Field(min_length=64, max_length=64)
    selected_observation_mappings: tuple[SelectedObservationMappingReference, ...] = Field(min_length=1)
    label_policy_bundle_id: str = Field(min_length=1, max_length=160)
    label_policy_bundle_hash: str = Field(min_length=64, max_length=64)
    label_source_revision_set_id: str = Field(min_length=1, max_length=160)
    label_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    label_as_of_ts: datetime
    planned_labels: tuple[PlannedLabelDescriptor, ...]
    planned_label_count: int = Field(ge=0)
    planned_label_hash: str = Field(min_length=64, max_length=64)
    data_source: str = "DB_HISTORICAL"
    execution_origin: str = "ADVISORY_RUN"
    research_scope: str = "HISTORICAL_RESEARCH_ONLY"
    execution_prohibited: bool = True
    capture_request_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "source_capture_receipt_hash",
        "source_capture_membership_hash",
        "source_capture_plan_set_hash",
        "label_policy_bundle_hash",
        "label_source_revision_set_hash",
        "planned_label_hash",
        "capture_request_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("label_as_of_ts")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="label_as_of_ts")

    def canonical_payload(self) -> dict[str, Any]:
        binding_payload = self.binding.model_dump(
            mode="json",
            exclude={"capture_batch_id", "current_fencing_token", "binding_hash"},
        )
        return canonicalize(
            {
                "schema_version": self.schema_version,
                "capture_purpose": self.capture_purpose,
                "binding": binding_payload,
                "source_observation_capture_batch_id": self.source_observation_capture_batch_id,
                "source_capture_receipt_hash": self.source_capture_receipt_hash,
                "source_capture_membership_hash": self.source_capture_membership_hash,
                "source_capture_plan_set_count": self.source_capture_plan_set_count,
                "source_capture_plan_set_hash": self.source_capture_plan_set_hash,
                "selected_observation_mappings": [
                    item.model_dump(mode="json")
                    for item in sorted(self.selected_observation_mappings, key=lambda item: item.canonical_signal_id)
                ],
                "label_policy_bundle_id": self.label_policy_bundle_id,
                "label_policy_bundle_hash": self.label_policy_bundle_hash,
                "label_source_revision_set_id": self.label_source_revision_set_id,
                "label_source_revision_set_hash": self.label_source_revision_set_hash,
                "label_as_of_ts": self.label_as_of_ts,
                "planned_labels": [
                    item.model_dump(mode="json")
                    for item in sorted(
                        self.planned_labels,
                        key=lambda item: (item.canonical_signal_id, item.symbol, item.horizon_trading_days, item.projection),
                    )
                ],
                "planned_label_count": self.planned_label_count,
                "planned_label_hash": self.planned_label_hash,
                "data_source": self.data_source,
                "execution_origin": self.execution_origin,
                "research_scope": self.research_scope,
                "execution_prohibited": self.execution_prohibited,
            }
        )

    @model_validator(mode="after")
    def _frozen_request(self) -> "LabelCaptureBatchRequestV2":
        if self.schema_version != LABEL_CAPTURE_BATCH_SCHEMA_VERSION or self.capture_purpose != LABEL_CAPTURE_PURPOSE:
            raise ValueError("unsupported label capture request schema or purpose")
        if (
            self.data_source != "DB_HISTORICAL"
            or self.execution_origin != "ADVISORY_RUN"
            or self.research_scope != "HISTORICAL_RESEARCH_ONLY"
            or self.execution_prohibited is not True
        ):
            raise ValueError("label capture requests are restricted to historical advisory research")
        if self.binding.capture_batch_id != self.capture_batch_id or self.binding.current_fencing_token != 1:
            raise ValueError("new label capture binding must reference this batch with fencing token one")
        binding = self.binding
        if (
            self.source_observation_capture_batch_id != binding.source_observation_capture_batch_id
            or self.source_capture_receipt_hash != binding.source_capture_receipt_hash
            or self.source_capture_membership_hash != binding.source_capture_membership_hash
            or self.source_capture_plan_set_count != binding.source_capture_plan_set_count
            or self.source_capture_plan_set_hash != binding.source_capture_plan_set_hash
            or self.label_policy_bundle_id != binding.label_policy_bundle_id
            or self.label_policy_bundle_hash != binding.label_policy_bundle_hash
            or self.label_source_revision_set_id != binding.label_source_revision_set_id
            or self.label_source_revision_set_hash != binding.label_source_revision_set_hash
            or self.label_as_of_ts != binding.label_as_of_ts
        ):
            raise ValueError("label capture request does not match its binding")
        mapping_count, mapping_hash = _mapping_set_hash(self.selected_observation_mappings)
        if (
            mapping_count != binding.selected_observation_mapping_set_count
            or mapping_hash != binding.selected_observation_mapping_set_hash
        ):
            raise ValueError("label capture mapping set does not match its binding")
        keys = [
            (item.canonical_signal_id, item.symbol, item.horizon_trading_days, item.projection)
            for item in self.planned_labels
        ]
        if len(set(keys)) != len(keys):
            raise ValueError("planned labels must have unique signal/symbol/horizon/projection identities")
        planned_count, planned_hash = _planned_label_set_hash(self.planned_labels)
        if planned_count != self.planned_label_count or planned_hash != self.planned_label_hash:
            raise ValueError("planned label count or hash does not match descriptors")
        digest = canonical_json_sha256(self.canonical_payload())
        if self.capture_request_hash is not None and self.capture_request_hash != digest:
            raise ValueError("capture_request_hash does not match label capture request")
        object.__setattr__(self, "capture_request_hash", digest)
        return self


def build_label_capture_request(
    context: LabelCaptureAdmissionContext,
    *,
    capture_batch_id: str,
    planned_labels: Iterable[PlannedLabelDescriptor],
    current_fencing_token: int = 1,
) -> LabelCaptureBatchRequestV2:
    """Build a v2 request from one context without mutable-current reads."""

    binding = build_label_capture_binding(
        context,
        capture_batch_id=capture_batch_id,
        current_fencing_token=current_fencing_token,
    )
    descriptors = tuple(planned_labels)
    planned_count, planned_hash = _planned_label_set_hash(descriptors)
    return LabelCaptureBatchRequestV2(
        capture_batch_id=capture_batch_id,
        binding=binding,
        source_observation_capture_batch_id=binding.source_observation_capture_batch_id,
        source_capture_receipt_hash=binding.source_capture_receipt_hash,
        source_capture_membership_hash=binding.source_capture_membership_hash,
        source_capture_plan_set_count=binding.source_capture_plan_set_count,
        source_capture_plan_set_hash=binding.source_capture_plan_set_hash,
        selected_observation_mappings=context.mapping_refs(),
        label_policy_bundle_id=binding.label_policy_bundle_id,
        label_policy_bundle_hash=binding.label_policy_bundle_hash,
        label_source_revision_set_id=binding.label_source_revision_set_id,
        label_source_revision_set_hash=binding.label_source_revision_set_hash,
        label_as_of_ts=binding.label_as_of_ts,
        planned_labels=descriptors,
        planned_label_count=planned_count,
        planned_label_hash=planned_hash,
    )
