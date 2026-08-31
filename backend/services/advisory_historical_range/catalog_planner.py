"""Deterministic, repository-free source catalog chunk planning for Phase 1R."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeCatalogMemberDeltaV1,
    HistoricalRangeCatalogPhase,
    HistoricalRangeContractError,
    HistoricalRangeSourceCatalogCheckpointV1,
    HistoricalRangeSourceRequirementPlanV1,
    HistoricalRangeSourceRequirementV1,
    HistoricalRangeSourceRevisionMemberV1,
    HistoricalRangeUnresolvedRequirementV1,
    append_catalog_member_chain_hash,
)


REASON_SOURCE_INPUT_UNAVAILABLE = "ADVISORY_HR_PIT_INPUT_UNAVAILABLE"
REASON_SOURCE_REVISION_DRIFT = "ADVISORY_HR_SOURCE_REVISION_DRIFT"
REASON_CATALOG_CHECKPOINT_CONFLICT = "ADVISORY_HR_CATALOG_CHECKPOINT_CONFLICT"
REASON_BLOCKED_BY_REQUIREMENT = "ADVISORY_HR_BLOCKED_BY_REQUIREMENT"


class HistoricalRangeSourceInputUnavailable(RuntimeError):
    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = str(reason_code or REASON_SOURCE_INPUT_UNAVAILABLE)
        self.context = context or {}


class HistoricalRangeSourceRequirementResolver(Protocol):
    def resolve(
        self,
        *,
        requirement: HistoricalRangeSourceRequirementV1,
        dependency_members: Mapping[str, HistoricalRangeSourceRevisionMemberV1],
        phase: HistoricalRangeCatalogPhase,
        expected_member: HistoricalRangeSourceRevisionMemberV1 | None,
    ) -> HistoricalRangeSourceRevisionMemberV1: ...


@dataclass(frozen=True)
class HistoricalRangeCatalogChunkResult:
    checkpoint: HistoricalRangeSourceCatalogCheckpointV1
    resolved_members: Mapping[str, HistoricalRangeSourceRevisionMemberV1]
    waiting_input: bool
    phase_complete: bool


class HistoricalRangeCatalogPlanner:
    """Resolve at most one stable chunk and stop exactly at the first missing input."""

    def resolve_chunk(
        self,
        *,
        plan: HistoricalRangeSourceRequirementPlanV1,
        catalog_generation: int,
        phase: HistoricalRangeCatalogPhase,
        start_ordinal: int,
        resolver: HistoricalRangeSourceRequirementResolver,
        resolved_members: Mapping[str, HistoricalRangeSourceRevisionMemberV1],
        expected_members: Mapping[str, HistoricalRangeSourceRevisionMemberV1] | None = None,
        previous_checkpoint_ref: HistoricalRangeArtifactRefV1 | None = None,
        previous_checkpoint: HistoricalRangeSourceCatalogCheckpointV1 | None = None,
        chunk_size: int = 32,
    ) -> HistoricalRangeCatalogChunkResult:
        if not 1 <= chunk_size <= 32:
            raise ValueError("catalog chunk_size must be between 1 and 32")
        if not 1 <= start_ordinal <= len(plan.requirements):
            raise ValueError("start_ordinal must reference a planned requirement")
        if catalog_generation < 1:
            raise ValueError("catalog_generation must be positive")
        if phase is HistoricalRangeCatalogPhase.VERIFY and expected_members is None:
            raise ValueError("VERIFY requires the complete DISCOVER member set")
        if phase is HistoricalRangeCatalogPhase.DISCOVER and expected_members is not None:
            raise ValueError("DISCOVER cannot accept a pre-resolved verification set")
        accumulated = dict(resolved_members)
        if len(accumulated) != len(set(accumulated)):
            raise ValueError("resolved_members requirement identities must be unique")
        self._validate_previous_checkpoint(
            plan=plan,
            catalog_generation=catalog_generation,
            phase=phase,
            start_ordinal=start_ordinal,
            resolved_members=accumulated,
            previous_checkpoint_ref=previous_checkpoint_ref,
            previous_checkpoint=previous_checkpoint,
        )
        dependency_source = dict(expected_members or accumulated)
        member_delta: list[HistoricalRangeCatalogMemberDeltaV1] = []
        unresolved_delta: list[HistoricalRangeUnresolvedRequirementV1] = []
        chain_hash = (
            previous_checkpoint.cumulative_member_chain_hash
            if previous_checkpoint is not None and previous_checkpoint.phase is phase
            else canonical_json_sha256([])
        )
        ordinal_end = min(len(plan.requirements), start_ordinal + chunk_size - 1)
        actual_end = start_ordinal - 1
        for ordinal in range(start_ordinal, ordinal_end + 1):
            requirement = plan.requirements[ordinal - 1]
            actual_end = ordinal
            missing_dependencies = tuple(
                dependency
                for dependency in requirement.depends_on_requirement_ids
                if dependency not in dependency_source
            )
            if missing_dependencies:
                unresolved_delta.append(
                    HistoricalRangeUnresolvedRequirementV1(
                        ordinal=ordinal,
                        requirement_id=requirement.requirement_id,
                        reason_code=REASON_BLOCKED_BY_REQUIREMENT,
                        blocked_by_requirement_ids=missing_dependencies,
                    )
                )
                break
            expected = (expected_members or {}).get(requirement.requirement_id)
            try:
                member = resolver.resolve(
                    requirement=requirement,
                    dependency_members={
                        dependency: dependency_source[dependency]
                        for dependency in requirement.depends_on_requirement_ids
                    },
                    phase=phase,
                    expected_member=expected,
                )
            except HistoricalRangeSourceInputUnavailable as exc:
                unresolved_delta.append(
                    HistoricalRangeUnresolvedRequirementV1(
                        ordinal=ordinal,
                        requirement_id=requirement.requirement_id,
                        reason_code=exc.reason_code,
                        context=exc.context,
                    )
                )
                break
            self._validate_member(requirement=requirement, member=member)
            if expected is not None and member.revision_hash != expected.revision_hash:
                raise HistoricalRangeContractError(
                    REASON_SOURCE_REVISION_DRIFT,
                    "VERIFY resolved a different source revision",
                    context={
                        "requirement_id": requirement.requirement_id,
                        "expected_revision_hash": expected.revision_hash,
                        "actual_revision_hash": member.revision_hash,
                        "catalog_generation": catalog_generation,
                    },
                )
            accumulated[requirement.requirement_id] = member
            dependency_source[requirement.requirement_id] = member
            member_delta.append(HistoricalRangeCatalogMemberDeltaV1(ordinal=ordinal, member=member))
            chain_hash = append_catalog_member_chain_hash(
                previous_chain_hash=chain_hash,
                ordinal=ordinal,
                member=member,
            )
        checkpoint = HistoricalRangeSourceCatalogCheckpointV1(
            requirement_plan_hash=str(plan.requirement_plan_hash),
            catalog_generation=catalog_generation,
            phase=phase,
            ordinal_start=start_ordinal,
            ordinal_end=actual_end,
            next_requirement_ordinal=(unresolved_delta[0].ordinal if unresolved_delta else actual_end + 1),
            previous_checkpoint_ref=previous_checkpoint_ref,
            previous_checkpoint_hash=(
                previous_checkpoint_ref.semantic_content_hash if previous_checkpoint_ref is not None else None
            ),
            member_delta=tuple(member_delta),
            unresolved_requirement_delta=tuple(unresolved_delta),
            cumulative_resolved_count=len(accumulated),
            cumulative_member_chain_hash=chain_hash,
        )
        waiting = bool(unresolved_delta)
        return HistoricalRangeCatalogChunkResult(
            checkpoint=checkpoint,
            resolved_members=accumulated,
            waiting_input=waiting,
            phase_complete=not waiting and checkpoint.next_requirement_ordinal == len(plan.requirements) + 1,
        )

    @staticmethod
    def _validate_previous_checkpoint(
        *,
        plan: HistoricalRangeSourceRequirementPlanV1,
        catalog_generation: int,
        phase: HistoricalRangeCatalogPhase,
        start_ordinal: int,
        resolved_members: Mapping[str, HistoricalRangeSourceRevisionMemberV1],
        previous_checkpoint_ref: HistoricalRangeArtifactRefV1 | None,
        previous_checkpoint: HistoricalRangeSourceCatalogCheckpointV1 | None,
    ) -> None:
        if (previous_checkpoint_ref is None) != (previous_checkpoint is None):
            raise HistoricalRangeContractError(
                REASON_CATALOG_CHECKPOINT_CONFLICT,
                "previous checkpoint ref/payload must be supplied together",
            )
        if previous_checkpoint is None:
            if start_ordinal != 1 or resolved_members:
                raise HistoricalRangeContractError(
                    REASON_CATALOG_CHECKPOINT_CONFLICT,
                    "first catalog chunk must start at ordinal 1 with no accumulated members",
                )
            return
        if previous_checkpoint_ref.artifact_kind is not HistoricalRangeArtifactKind.SOURCE_CATALOG_CHECKPOINT:
            raise HistoricalRangeContractError(
                REASON_CATALOG_CHECKPOINT_CONFLICT,
                "previous checkpoint ref has the wrong artifact kind",
            )
        if previous_checkpoint_ref.payload_sha256 != canonical_json_sha256(previous_checkpoint.model_dump(mode="json")):
            raise HistoricalRangeContractError(
                REASON_CATALOG_CHECKPOINT_CONFLICT,
                "previous checkpoint ref does not close the supplied checkpoint payload",
            )
        same_phase = previous_checkpoint.phase is phase
        phase_handoff = (
            previous_checkpoint.phase is HistoricalRangeCatalogPhase.DISCOVER
            and phase is HistoricalRangeCatalogPhase.VERIFY
            and start_ordinal == 1
            and previous_checkpoint.next_requirement_ordinal == len(plan.requirements) + 1
            and not resolved_members
        )
        if (
            previous_checkpoint.requirement_plan_hash != plan.requirement_plan_hash
            or previous_checkpoint.catalog_generation != catalog_generation
            or not (same_phase or phase_handoff)
            or (same_phase and previous_checkpoint.next_requirement_ordinal != start_ordinal)
            or (same_phase and previous_checkpoint.cumulative_resolved_count != len(resolved_members))
        ):
            raise HistoricalRangeContractError(
                REASON_CATALOG_CHECKPOINT_CONFLICT,
                "previous checkpoint does not match the requested catalog cursor",
            )

    @staticmethod
    def _validate_member(
        *,
        requirement: HistoricalRangeSourceRequirementV1,
        member: HistoricalRangeSourceRevisionMemberV1,
    ) -> None:
        expected = {
            "requirement_id": requirement.requirement_id,
            "source_role": requirement.source_role,
            "dataset_id": requirement.dataset_id,
            "package_id": requirement.package_id,
            "component_id": requirement.component_id,
            "decision_trade_date": requirement.decision_trade_date,
            "query_template_id": requirement.query_template_id,
            "query_template_version": requirement.query_template_version,
            "query_template_hash": requirement.query_template_hash,
        }
        mismatches = {
            key: {"expected": value, "actual": getattr(member, key)}
            for key, value in expected.items()
            if getattr(member, key) != value
        }
        if mismatches:
            raise HistoricalRangeContractError(
                REASON_CATALOG_CHECKPOINT_CONFLICT,
                "resolved source member differs from its requirement identity",
                context={"requirement_id": requirement.requirement_id, "mismatches": mismatches},
            )
