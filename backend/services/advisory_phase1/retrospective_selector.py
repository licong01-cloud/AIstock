"""Range-only Phase 1 retrospective selector with no formal fallback."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeLineageIdentity,
    REASON_DATASET_BRIDGE_FORMAL_FALLBACK_FORBIDDEN,
    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
    require_sha256,
)


RETROSPECTIVE_SELECTOR_POLICY_VERSION = "advisory_phase1_retrospective_selector_policy_v1"
RETROSPECTIVE_SELECTOR_POLICY_HASH = canonical_json_sha256(
    {
        "schema_version": RETROSPECTIVE_SELECTOR_POLICY_VERSION,
        "accepted_lineage": "HISTORICAL_RANGE_RESEARCH",
        "accepted_scope": "RETROSPECTIVE_RESEARCH_ONLY",
        "resolution": "EXACT_RANGE_DAY_CANDIDATE_OUTCOME_REFS_V1",
        "canonical_signal_dedup": "ECONOMIC_SIGNAL_ID_WITH_ALL_LINEAGE_REFS_V1",
        "fallback": "forbidden",
    }
)


def _aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


class RetrospectiveObservationVersion(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    canonical_signal_id: str = Field(min_length=1, max_length=160)
    observation_version_id: str = Field(min_length=1, max_length=160)
    observation_content_hash: str = Field(min_length=64, max_length=64)
    evidence_available_at: datetime
    execution_origin: Literal["HISTORICAL_RANGE_RESEARCH"] = "HISTORICAL_RANGE_RESEARCH"
    evidence_scope: Literal["RETROSPECTIVE_RESEARCH_ONLY"] = "RETROSPECTIVE_RESEARCH_ONLY"
    lineage_source_type: Literal["HISTORICAL_RANGE_RESEARCH"] = "HISTORICAL_RANGE_RESEARCH"
    lineage: HistoricalRangeLineageIdentity
    candidate_artifact_ref: HistoricalRangeArtifactRefV1
    outcome_refs: tuple[HistoricalRangeArtifactRefV1, ...]
    observation_payload: dict[str, Any]

    @field_validator("observation_content_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return require_sha256(value, field_name="observation_content_hash")

    @field_validator("evidence_available_at")
    @classmethod
    def _timestamp(cls, value: datetime) -> datetime:
        return _aware(value, field_name="evidence_available_at")

    @model_validator(mode="after")
    def _closure(self) -> "RetrospectiveObservationVersion":
        if self.candidate_artifact_ref != self.lineage.candidate_artifact_ref:
            raise ValueError("observation candidate ref differs from range lineage")
        if any(item.artifact_kind is not HistoricalRangeArtifactKind.OUTCOME for item in self.outcome_refs):
            raise ValueError("retrospective observation outcome refs must be OUTCOME")
        refs = tuple(sorted(self.outcome_refs, key=lambda item: item.semantic_content_hash))
        if len(refs) != len({item.semantic_content_hash for item in refs}):
            raise ValueError("retrospective observation outcome refs must be unique")
        payload_hash = canonical_json_sha256(self.observation_payload)
        if payload_hash != self.observation_content_hash:
            raise ValueError("observation_content_hash does not match immutable payload")
        if self.observation_version_id != f"osv_{payload_hash[:20]}":
            raise ValueError("observation_version_id does not match immutable payload")
        object.__setattr__(self, "outcome_refs", refs)
        return self


class RetrospectiveSelectionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    range_run_ids: tuple[str, ...] = Field(min_length=1)
    candidate_artifact_refs: tuple[HistoricalRangeArtifactRefV1, ...] = Field(
        min_length=1
    )
    outcome_refs: tuple[HistoricalRangeArtifactRefV1, ...] = Field(min_length=1)
    requested_source_cutoff: datetime
    execution_origin: Literal["HISTORICAL_RANGE_RESEARCH"] = "HISTORICAL_RANGE_RESEARCH"
    evidence_scope: Literal["RETROSPECTIVE_RESEARCH_ONLY"] = "RETROSPECTIVE_RESEARCH_ONLY"
    selector_policy_hash: str = RETROSPECTIVE_SELECTOR_POLICY_HASH
    selector_request_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("selector_policy_hash", "selector_request_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("requested_source_cutoff")
    @classmethod
    def _timestamp(cls, value: datetime) -> datetime:
        return _aware(value, field_name="requested_source_cutoff")

    @model_validator(mode="after")
    def _identity(self) -> "RetrospectiveSelectionRequest":
        if self.selector_policy_hash != RETROSPECTIVE_SELECTOR_POLICY_HASH:
            raise ValueError("retrospective selector policy hash is invalid")
        if self.range_run_ids != tuple(sorted(set(self.range_run_ids))):
            raise ValueError("range_run_ids must be sorted and unique")
        for field_name, refs, kind in (
            ("candidate_artifact_refs", self.candidate_artifact_refs, HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT),
            ("outcome_refs", self.outcome_refs, HistoricalRangeArtifactKind.OUTCOME),
        ):
            ordered = tuple(sorted(refs, key=lambda item: item.semantic_content_hash))
            if ordered != refs or len(ordered) != len({item.semantic_content_hash for item in ordered}):
                raise ValueError(f"{field_name} must be sorted and unique")
            if any(item.artifact_kind is not kind for item in ordered):
                raise ValueError(f"{field_name} has an invalid artifact kind")
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"selector_request_hash"}))
        if self.selector_request_hash is not None and self.selector_request_hash != digest:
            raise ValueError("selector_request_hash does not match exact retrospective request")
        object.__setattr__(self, "selector_request_hash", digest)
        return self


class RetrospectiveSelectedObservationMapping(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    selector_request_hash: str = Field(min_length=64, max_length=64)
    selection_policy_hash: str = RETROSPECTIVE_SELECTOR_POLICY_HASH
    canonical_signal_id: str = Field(min_length=1, max_length=160)
    observation_version_id: str = Field(min_length=1, max_length=160)
    observation_content_hash: str = Field(min_length=64, max_length=64)
    selected_lineage_refs: tuple[str, ...] = Field(min_length=1)
    accepted_outcome_refs: tuple[HistoricalRangeArtifactRefV1, ...] = Field(min_length=1)
    selection_status: Literal["SELECTED"] = "SELECTED"
    selected_mapping_hash: str | None = Field(default=None, min_length=64, max_length=64)
    selected_mapping_id: str | None = Field(default=None, min_length=1, max_length=160)

    @field_validator(
        "selector_request_hash", "selection_policy_hash", "observation_content_hash", "selected_mapping_hash"
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "RetrospectiveSelectedObservationMapping":
        if self.selection_policy_hash != RETROSPECTIVE_SELECTOR_POLICY_HASH:
            raise ValueError("retrospective mapping policy hash is invalid")
        if self.selected_lineage_refs != tuple(sorted(set(self.selected_lineage_refs))):
            raise ValueError("selected lineage refs must be sorted and unique")
        outcome_refs = tuple(
            sorted(self.accepted_outcome_refs, key=lambda item: item.semantic_content_hash)
        )
        if (
            outcome_refs != self.accepted_outcome_refs
            or len(outcome_refs)
            != len({item.semantic_content_hash for item in outcome_refs})
            or any(
                item.artifact_kind is not HistoricalRangeArtifactKind.OUTCOME
                for item in outcome_refs
            )
        ):
            raise ValueError(
                "selected outcome refs must be sorted unique OUTCOME refs"
            )
        digest = canonical_json_sha256(
            self.model_dump(mode="json", exclude={"selected_mapping_hash", "selected_mapping_id"})
        )
        expected_id = f"rsom_{digest[:20]}"
        if self.selected_mapping_hash is not None and self.selected_mapping_hash != digest:
            raise ValueError("selected_mapping_hash does not match retrospective mapping")
        if self.selected_mapping_id is not None and self.selected_mapping_id != expected_id:
            raise ValueError("selected_mapping_id does not match retrospective mapping")
        object.__setattr__(self, "selected_mapping_hash", digest)
        object.__setattr__(self, "selected_mapping_id", expected_id)
        return self


class RetrospectiveObservationSelector:
    def select(
        self,
        *,
        request: RetrospectiveSelectionRequest,
        observations: Iterable[RetrospectiveObservationVersion],
    ) -> tuple[RetrospectiveSelectedObservationMapping, ...]:
        candidates = tuple(observations)
        if any(item.lineage_source_type != "HISTORICAL_RANGE_RESEARCH" for item in candidates):
            raise ValueError(REASON_DATASET_BRIDGE_FORMAL_FALLBACK_FORBIDDEN)
        expected_candidates = {item.semantic_content_hash for item in request.candidate_artifact_refs}
        expected_outcomes = {item.semantic_content_hash for item in request.outcome_refs}
        observed_candidates: set[str] = set()
        observed_outcomes: set[str] = set()
        selected: dict[str, list[RetrospectiveObservationVersion]] = {}
        for observation in candidates:
            if observation.evidence_available_at > request.requested_source_cutoff:
                continue
            lineage = observation.lineage
            if lineage.range_run_id not in request.range_run_ids:
                raise ValueError(REASON_DATASET_BRIDGE_LINEAGE_CONFLICT)
            if observation.candidate_artifact_ref.semantic_content_hash not in expected_candidates:
                raise ValueError(REASON_DATASET_BRIDGE_LINEAGE_CONFLICT)
            outcome_hashes = {item.semantic_content_hash for item in observation.outcome_refs}
            if not outcome_hashes or not outcome_hashes <= expected_outcomes:
                raise ValueError(REASON_DATASET_BRIDGE_LINEAGE_CONFLICT)
            observed_candidates.add(
                observation.candidate_artifact_ref.semantic_content_hash
            )
            observed_outcomes.update(outcome_hashes)
            selected.setdefault(observation.canonical_signal_id, []).append(observation)
        if (
            observed_candidates != expected_candidates
            or observed_outcomes != expected_outcomes
        ):
            raise ValueError(REASON_DATASET_BRIDGE_LINEAGE_CONFLICT)
        mappings: list[RetrospectiveSelectedObservationMapping] = []
        for signal_id, versions in sorted(selected.items()):
            versions.sort(key=lambda item: (item.evidence_available_at, item.observation_version_id))
            terminal = versions[-1]
            if any(
                item.observation_content_hash != terminal.observation_content_hash
                for item in versions[:-1]
            ):
                # Multiple range lineages may point at one economic sample, but the
                # economic observation payload itself must remain identical.
                raise ValueError(REASON_DATASET_BRIDGE_LINEAGE_CONFLICT)
            lineage_refs = tuple(
                sorted(str(item.lineage.range_lineage_identity_hash) for item in versions)
            )
            outcome_refs_by_hash = {
                ref.semantic_content_hash: ref for item in versions for ref in item.outcome_refs
            }
            mappings.append(
                RetrospectiveSelectedObservationMapping(
                    selector_request_hash=str(request.selector_request_hash),
                    canonical_signal_id=signal_id,
                    observation_version_id=terminal.observation_version_id,
                    observation_content_hash=terminal.observation_content_hash,
                    selected_lineage_refs=lineage_refs,
                    accepted_outcome_refs=tuple(outcome_refs_by_hash[key] for key in sorted(outcome_refs_by_hash)),
                )
            )
        return tuple(mappings)


def __getattr__(name: str) -> Any:
    if name == "PostgresRetrospectiveObservationSelector":
        from backend.services.advisory_phase1.retrospective_selector_postgres import (
            PostgresRetrospectiveObservationSelector,
        )

        return PostgresRetrospectiveObservationSelector
    raise AttributeError(name)
