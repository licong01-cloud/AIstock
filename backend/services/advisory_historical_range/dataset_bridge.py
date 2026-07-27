"""Phase 1 retrospective bridge for exact Phase 1R candidate/outcome facts."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import logging
from typing import Any, Callable, Iterable, Literal, Mapping, Protocol
from uuid import uuid4

import psycopg2

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    DATASET_BRIDGE_RECEIPT_SCHEMA_VERSION,
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeBridgeResultStatus,
    HistoricalRangeDatasetBridgeReceiptV1,
    HistoricalRangeDatasetBridgeRequestV1,
    HistoricalRangeContractError,
    HistoricalRangeEvaluationWindowType,
    HistoricalRangeLineageIdentity,
    HistoricalRangeOperationAttemptV1,
    HistoricalRangeOperationRequestV1,
    HistoricalRangeOperationStatus,
    HistoricalRangeOperationType,
    HistoricalRangeOutcomePolicyBundleV1,
    HistoricalRangeOutcomeFactV1,
    HistoricalRangeOutcomeArtifactV2,
    HistoricalRangeOutcomeProjection,
    HistoricalRangeOutcomeStatus,
    HistoricalRangeOutcomeSubjectType,
    REASON_DATABASE_CAPACITY_EXHAUSTED,
    REASON_DATABASE_UNAVAILABLE,
    REASON_DATASET_BRIDGE_FAILED,
    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
    REASON_DATASET_BRIDGE_VALID_EMPTY,
    REASON_REPOSITORY_CONFLICT,
    derive_prefixed_id,
    require_sha256,
)
from backend.services.advisory_historical_range.outcome_projection import (
    map_historical_range_maturity,
)
from backend.services.advisory_phase1.retrospective_selector import RETROSPECTIVE_SELECTOR_POLICY_HASH
from backend.services.advisory_phase1.capture_foundation import (
    RetrospectiveObservationCapturePlan,
)
from backend.services.advisory_phase1.observation_capture import (
    materialize_retrospective_observation_row_bundle,
    retrospective_observation_payload,
)
from backend.services.advisory_phase1.outcome_engine import (
    CalculationEvidenceBundle,
    OutcomeCalculationResult,
    OutcomeOwner,
)
from backend.services.advisory_phase1.label_policy import Projection
from backend.services.advisory_historical_range.retrospective_projection import (
    PostgresHistoricalRangeCandidateProjectionLoader,
)


logger = logging.getLogger(__name__)


class HistoricalRangeDatasetBridgeError(RuntimeError):
    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


class HistoricalRangeBridgeCandidateV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    canonical_signal_id: str = Field(min_length=1, max_length=160)
    symbol: str = Field(min_length=1, max_length=32)
    lineage: HistoricalRangeLineageIdentity
    candidate_artifact_ref: HistoricalRangeArtifactRefV1
    capture_plan: RetrospectiveObservationCapturePlan
    candidate_fact: dict[str, Any]
    owner: OutcomeOwner
    stage_payload: dict[str, Any]
    stage_payload_hash: str = Field(min_length=64, max_length=64)
    outcome: HistoricalRangeOutcomeFactV1
    outcome_ref: HistoricalRangeArtifactRefV1

    @field_validator("stage_payload_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return require_sha256(value, field_name="stage_payload_hash")

    @field_validator("symbol")
    @classmethod
    def _symbol(cls, value: str) -> str:
        return value.strip().upper()

    @model_validator(mode="after")
    def _closure(self) -> "HistoricalRangeBridgeCandidateV1":
        if self.candidate_artifact_ref != self.lineage.candidate_artifact_ref:
            raise ValueError("bridge candidate ref differs from range lineage")
        if canonical_json_sha256(self.stage_payload) != self.stage_payload_hash:
            raise ValueError("stage payload hash differs from R3 exact stages")
        if (
            self.capture_plan.canonical_signal_id != self.canonical_signal_id
            or self.capture_plan.symbol != self.symbol
            or self.owner.canonical_signal_id != self.canonical_signal_id
            or self.owner.symbol != self.symbol
            or self.owner.owner_key != self.outcome.subject_id
        ):
            raise ValueError("bridge observation plan/owner differs from candidate identity")
        if (
            self.capture_plan.lineage.model_dump(mode="json")
            != self.lineage.model_dump(mode="json")
        ):
            raise ValueError("bridge capture plan lineage differs from range lineage")
        if self.outcome_ref != self.outcome.outcome_artifact_ref:
            raise ValueError("bridge outcome ref differs from durable outcome fact")
        try:
            artifact = HistoricalRangeOutcomeArtifactV2.model_validate(
                self.outcome.outcome_json
            )
        except ValueError as exc:
            raise ValueError(
                "durable outcome fact must embed a complete V2 outcome artifact"
            ) from exc
        if (
            artifact.subject_ref != self.candidate_artifact_ref
            or artifact.outcome_logical_id != self.outcome.outcome_logical_id
            or artifact.outcome_version_id != self.outcome.outcome_version_id
            or artifact.outcome_input_hash != self.outcome.outcome_input_hash
            or artifact.projection_group is not self.outcome.projection
            or artifact.evaluation_window_type is not self.outcome.evaluation_window_type
            or artifact.horizon_trade_days != self.outcome.horizon_trade_days
            or artifact.policy_bundle_hash
            != self.outcome.historical_range_policy_bundle_hash
            or artifact.source_revision_set_hash != self.outcome.source_revision_set_hash
            or artifact.maturity_status is not self.outcome.maturity_status
            or artifact.label_as_of_trade_date
            != self.outcome.label_as_of_trade_date
            or artifact.next_refresh_trade_date
            != self.outcome.next_refresh_trade_date
            or artifact.producer_code_hash != self.outcome.producer_code_hash
        ):
            raise ValueError("embedded outcome artifact differs from durable outcome fact")
        if (
            self.outcome.subject_type is not HistoricalRangeOutcomeSubjectType.CANDIDATE
            or self.outcome.projection is not HistoricalRangeOutcomeProjection.EXECUTABLE
            or self.outcome.evaluation_window_type is not HistoricalRangeEvaluationWindowType.FIXED_HORIZON
        ):
            raise ValueError(
                "Phase 1 labels accept only candidate fixed-horizon executable outcomes"
            )
        return self


class HistoricalRangeBridgeObservationV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    canonical_signal_id: str
    observation_version_id: str
    observation_content_hash: str
    lineage: HistoricalRangeLineageIdentity
    capture_plan: RetrospectiveObservationCapturePlan
    candidate_fact: dict[str, Any]
    owner: OutcomeOwner
    observation_payload: dict[str, Any]
    stage_payload: dict[str, Any]
    lineage_variants: tuple[HistoricalRangeLineageIdentity, ...] = ()
    capture_plan_variants: tuple[RetrospectiveObservationCapturePlan, ...] = ()
    accepted_outcome_refs: tuple[HistoricalRangeArtifactRefV1, ...] = ()
    evidence_scope: str = "RETROSPECTIVE_RESEARCH_ONLY"
    selector_policy_hash: str = RETROSPECTIVE_SELECTOR_POLICY_HASH

    @model_validator(mode="after")
    def _closure(self) -> "HistoricalRangeBridgeObservationV1":
        digest = canonical_json_sha256(self.observation_payload)
        if (
            self.canonical_signal_id != self.capture_plan.canonical_signal_id
            or self.owner.canonical_signal_id != self.canonical_signal_id
            or self.owner.observation_version_id != self.observation_version_id
            or self.observation_content_hash != digest
            or self.observation_version_id != f"osv_{digest[:20]}"
        ):
            raise ValueError("bridge observation identity differs from materialization")
        lineages = self.lineage_variants or (self.lineage,)
        plans = self.capture_plan_variants or (self.capture_plan,)
        refs = self.accepted_outcome_refs or ()
        if len(lineages) != len(plans) or any(
            plan.lineage.model_dump(mode="json") != lineage.model_dump(mode="json")
            for plan, lineage in zip(plans, lineages, strict=True)
        ):
            raise ValueError("bridge observation lineage/plan variants are not closed")
        if any(ref.artifact_kind is not HistoricalRangeArtifactKind.OUTCOME for ref in refs):
            raise ValueError("bridge observation accepted refs must be OUTCOME artifacts")
        object.__setattr__(self, "lineage_variants", tuple(lineages))
        object.__setattr__(self, "capture_plan_variants", tuple(plans))
        object.__setattr__(self, "accepted_outcome_refs", tuple(sorted(set(refs), key=lambda ref: ref.semantic_content_hash)))
        return self


class HistoricalRangeBridgeLabelV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    canonical_signal_id: str
    label_version_id: str
    label_content_hash: str
    observation_version_id: str
    symbol: str
    projection: Projection
    horizon_trade_days: int
    outcome_version_id: str
    outcome_content_hash: str
    outcome_ref: HistoricalRangeArtifactRefV1
    label_as_of_trade_date: date | None = None
    accepted_outcome_refs: tuple[HistoricalRangeArtifactRefV1, ...] = ()
    historical_range_policy_bundle_hash: str
    historical_range_policy_bundle_ref: HistoricalRangeArtifactRefV1
    policy_component_set_hash: str
    outcome_result: OutcomeCalculationResult
    calculation_evidence: CalculationEvidenceBundle
    evidence_scope: str = "RETROSPECTIVE_RESEARCH_ONLY"

    @model_validator(mode="after")
    def _outcome_closure(self) -> "HistoricalRangeBridgeLabelV1":
        if self.outcome_result.owner.owner_type.value != "CANDIDATE":
            raise ValueError("retrospective labels require a candidate owner")
        if (
            self.outcome_ref.artifact_kind is not HistoricalRangeArtifactKind.OUTCOME
            or
            self.outcome_result.projection is not self.projection
            or self.outcome_result.horizon_trading_days != self.horizon_trade_days
            or self.outcome_result.calculation_evidence != self.calculation_evidence
        ):
            raise ValueError("bridge label differs from its exact calculation result")
        refs = self.accepted_outcome_refs or (self.outcome_ref,)
        if any(ref.artifact_kind is not HistoricalRangeArtifactKind.OUTCOME for ref in refs):
            raise ValueError("bridge label accepted refs must be OUTCOME artifacts")
        object.__setattr__(self, "accepted_outcome_refs", tuple(sorted(set(refs), key=lambda ref: ref.semantic_content_hash)))
        return self


class HistoricalRangeDatasetBridgeArtifactV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[
        "advisory_historical_range_dataset_bridge_artifact_v1"
    ] = "advisory_historical_range_dataset_bridge_artifact_v1"
    operation_id: str = Field(min_length=1, max_length=160)
    request: HistoricalRangeDatasetBridgeRequestV1
    request_hash: str = Field(min_length=64, max_length=64)
    result_status: HistoricalRangeBridgeResultStatus
    observations: tuple[HistoricalRangeBridgeObservationV1, ...] = ()
    labels: tuple[HistoricalRangeBridgeLabelV1, ...] = ()
    capture_ids: tuple[str, ...] = ()
    build_id: str | None = Field(default=None, min_length=1, max_length=160)
    sealed_snapshot_id: str | None = Field(default=None, min_length=1, max_length=160)
    selector_policy_hash: str = Field(min_length=64, max_length=64)
    capabilities: tuple[str, ...] = ()
    producer_code_hash: str = Field(min_length=64, max_length=64)

    @field_validator("request_hash", "selector_policy_hash", "producer_code_hash")
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return require_sha256(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _closure(self) -> "HistoricalRangeDatasetBridgeArtifactV1":
        if (
            self.request_hash != self.request.request_hash
            or self.selector_policy_hash
            != self.request.retrospective_selector_policy_hash
        ):
            raise ValueError("bridge artifact differs from its exact frozen request")
        if self.capture_ids != tuple(sorted(set(self.capture_ids))):
            raise ValueError("bridge capture ids must be sorted and duplicate-free")
        expected_capabilities = (
            ("RESEARCH_AUDIT", "INTERNAL_BOOTSTRAP")
            if self.observations
            else ()
        )
        if self.capabilities != expected_capabilities:
            raise ValueError("bridge capabilities differ from retrospective materialization")
        requested_outcomes = set(self.request.outcome_refs)
        requested_candidates = set(self.request.candidate_refs)
        requested_policies = set(self.request.policy_bundle_refs)
        observation_ids = {item.observation_version_id for item in self.observations}
        if len(observation_ids) != len(self.observations):
            raise ValueError("bridge artifact observations must be unique")
        for observation in self.observations:
            if (
                observation.lineage.range_run_id not in self.request.range_run_ids
                or observation.lineage.candidate_artifact_ref
                not in requested_candidates
                or not set(observation.accepted_outcome_refs) <= requested_outcomes
            ):
                raise ValueError("bridge observation lies outside the exact request")
        label_ids = {item.label_version_id for item in self.labels}
        if len(label_ids) != len(self.labels):
            raise ValueError("bridge artifact labels must be unique")
        for label in self.labels:
            if (
                label.observation_version_id not in observation_ids
                or label.historical_range_policy_bundle_ref
                not in requested_policies
                or not set(label.accepted_outcome_refs) <= requested_outcomes
            ):
                raise ValueError("bridge label lies outside the exact request")
        materialized = bool(self.observations)
        if self.result_status is HistoricalRangeBridgeResultStatus.SEALED:
            if (
                not materialized
                or not self.labels
                or not self.capture_ids
                or self.build_id is None
                or self.sealed_snapshot_id is None
            ):
                raise ValueError("SEALED bridge artifact requires full materialization")
        elif (
            materialized
            or self.labels
            or self.capture_ids
            or self.build_id is not None
            or self.sealed_snapshot_id is not None
        ):
            raise ValueError("non-SEALED bridge artifact cannot claim materialization")
        return self


class RetrospectiveCaptureWriter(Protocol):
    def capture(
        self,
        *,
        request: HistoricalRangeDatasetBridgeRequestV1,
        observations: tuple[HistoricalRangeBridgeObservationV1, ...],
        labels: tuple[HistoricalRangeBridgeLabelV1, ...],
    ) -> tuple[str, ...]: ...


class RetrospectiveDatasetBuilder(Protocol):
    def build(
        self,
        *,
        request: HistoricalRangeDatasetBridgeRequestV1,
        capture_ids: tuple[str, ...],
        observations: tuple[HistoricalRangeBridgeObservationV1, ...],
        labels: tuple[HistoricalRangeBridgeLabelV1, ...],
    ) -> str: ...


class RetrospectiveSnapshotWriter(Protocol):
    def seal(self, *, build_id: str, expected_selector_policy_hash: str) -> tuple[str, str]: ...


class HistoricalRangeBridgeInputLoader(Protocol):
    def load(
        self, *, request: HistoricalRangeDatasetBridgeRequestV1
    ) -> tuple[HistoricalRangeBridgeCandidateV1, ...]: ...


class HistoricalRangeBridgeOutcomeRepository(Protocol):
    def list_bridge_successful_days(
        self, *, day_receipt_hashes: tuple[str, ...]
    ) -> tuple[Mapping[str, Any], ...]: ...

    def list_bridge_candidate_outcomes(
        self, *, outcome_artifact_hashes: tuple[str, ...]
    ) -> tuple[tuple[str, HistoricalRangeOutcomeFactV1], ...]: ...


class PostgresHistoricalRangeBridgeInputLoader:
    """Resolve bridge inputs only from the exact request refs and durable R3/R4 facts."""

    def __init__(
        self,
        *,
        repository: HistoricalRangeBridgeOutcomeRepository,
        projection_loader: PostgresHistoricalRangeCandidateProjectionLoader,
    ) -> None:
        self._repository = repository
        self._projection_loader = projection_loader

    def load(
        self, *, request: HistoricalRangeDatasetBridgeRequestV1
    ) -> tuple[HistoricalRangeBridgeCandidateV1, ...]:
        day_refs = _exact_ref_map(
            request.successful_day_refs,
            field_name="successful_day_refs",
        )
        candidate_refs = _exact_ref_map(
            request.candidate_refs,
            field_name="candidate_refs",
        )
        outcome_refs = _exact_ref_map(
            request.outcome_refs,
            field_name="outcome_refs",
        )
        days = self._repository.list_bridge_successful_days(
            day_receipt_hashes=tuple(sorted(day_refs))
        )
        observed_day_hashes: set[str] = set()
        observed_candidate_refs: dict[str, HistoricalRangeArtifactRefV1] = {}
        included_candidate_keys: set[tuple[str, str]] = set()
        for raw_day in days:
            day = dict(raw_day)
            day_ref = HistoricalRangeArtifactRefV1.model_validate(
                day["day_receipt_ref"]
            )
            candidate_ref = HistoricalRangeArtifactRefV1.model_validate(
                day["candidate_artifact_ref"]
            )
            day_hash = day_ref.semantic_content_hash
            candidate_hash = candidate_ref.semantic_content_hash
            if (
                day_hash in observed_day_hashes
                or day_refs.get(day_hash) != day_ref
                or candidate_hash in observed_candidate_refs
                or candidate_refs.get(candidate_hash) != candidate_ref
                or str(day["range_run_id"]) not in request.range_run_ids
                or str(day["terminal_status"])
                not in {"COMPLETE", "VALID_NO_CANDIDATE"}
            ):
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "successful-day readback differs from the exact bridge request",
                )
            observed_day_hashes.add(day_hash)
            observed_candidate_refs[candidate_hash] = candidate_ref
            candidate_ids = tuple(str(value) for value in day["included_candidate_ids"])
            if len(candidate_ids) != len(set(candidate_ids)):
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "successful-day candidate membership is ambiguous",
                )
            if (str(day["terminal_status"]) == "VALID_NO_CANDIDATE") != (
                not candidate_ids
            ):
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "successful-day terminal status differs from candidate membership",
                )
            included_candidate_keys.update(
                (str(day["range_run_id"]), candidate_id)
                for candidate_id in candidate_ids
            )
        if (
            observed_day_hashes != set(day_refs)
            or set(observed_candidate_refs) != set(candidate_refs)
        ):
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "successful-day/candidate artifact coverage is incomplete",
            )
        if not outcome_refs:
            if included_candidate_keys:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "VALID_EMPTY requires a genuinely candidate-empty range selector",
                )
            return ()
        rows = self._repository.list_bridge_candidate_outcomes(
            outcome_artifact_hashes=tuple(sorted(outcome_refs))
        )
        policy_refs = {
            item.payload_sha256: item for item in request.policy_bundle_refs
        }
        loaded: list[HistoricalRangeBridgeCandidateV1] = []
        observed_outcome_hashes: set[str] = set()
        observed_selector_keys: set[tuple[str, str, int]] = set()
        for range_run_id, outcome in rows:
            outcome_hash = outcome.outcome_artifact_ref.semantic_content_hash
            selector_key = (
                range_run_id,
                outcome.subject_id,
                outcome.horizon_trade_days,
            )
            if (
                outcome_hash in observed_outcome_hashes
                or outcome_refs.get(outcome_hash) != outcome.outcome_artifact_ref
                or selector_key in observed_selector_keys
                or range_run_id not in request.range_run_ids
                or outcome.subject_type
                is not HistoricalRangeOutcomeSubjectType.CANDIDATE
                or outcome.projection
                is not HistoricalRangeOutcomeProjection.EXECUTABLE
                or outcome.evaluation_window_type
                is not HistoricalRangeEvaluationWindowType.FIXED_HORIZON
                or outcome.horizon_trade_days not in request.requested_horizons
                or not _eligible_executable_results(
                    outcome.outcome_json,
                    requested_maturity_statuses=request.requested_maturity_statuses,
                )
            ):
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "durable bridge outcome lies outside the exact request selector",
                )
            if (range_run_id, outcome.subject_id) not in included_candidate_keys:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "durable bridge outcome has no exact successful-day candidate",
                )
            observed_outcome_hashes.add(outcome_hash)
            observed_selector_keys.add(selector_key)
            policy_ref = policy_refs.get(
                outcome.historical_range_policy_bundle_hash
            )
            if policy_ref is None:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "durable bridge outcome uses an unrequested range policy",
                )
            projection = self._projection_loader.load(
                candidate_id=outcome.subject_id,
                range_run_id=range_run_id,
                policy_bundle_ref=policy_ref,
                policy_bundle_hash=outcome.historical_range_policy_bundle_hash,
            )
            if (
                projection.candidate_artifact_ref.semantic_content_hash
                not in candidate_refs
                or candidate_refs[
                    projection.candidate_artifact_ref.semantic_content_hash
                ]
                != projection.candidate_artifact_ref
            ):
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "durable bridge candidate artifact is outside the exact request",
                )
            loaded.append(
                HistoricalRangeBridgeCandidateV1(
                    canonical_signal_id=projection.capture_plan.canonical_signal_id,
                    symbol=projection.candidate_fact.symbol,
                    lineage=projection.lineage,
                    candidate_artifact_ref=projection.candidate_artifact_ref,
                    capture_plan=projection.capture_plan,
                    candidate_fact=projection.candidate_fact.model_dump(mode="python"),
                    owner=projection.owner,
                    stage_payload=projection.stage_payload,
                    stage_payload_hash=canonical_json_sha256(
                        projection.stage_payload
                    ),
                    outcome=outcome,
                    outcome_ref=outcome.outcome_artifact_ref,
                )
            )
        if observed_outcome_hashes != set(outcome_refs):
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "durable bridge outcome set is incomplete or conflicting",
            )
        return tuple(
            sorted(
                loaded,
                key=lambda item: (
                    item.canonical_signal_id,
                    item.outcome.horizon_trade_days,
                    item.outcome.outcome_version_id,
                ),
            )
        )


def _exact_ref_map(
    refs: Iterable[HistoricalRangeArtifactRefV1],
    *,
    field_name: str,
) -> dict[str, HistoricalRangeArtifactRefV1]:
    result: dict[str, HistoricalRangeArtifactRefV1] = {}
    for ref in refs:
        identity = ref.semantic_content_hash
        if identity in result:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                f"{field_name} must be duplicate-free",
            )
        result[identity] = ref
    return result


class HistoricalRangeDatasetBridgeService:
    def __init__(
        self,
        *,
        artifact_store: HistoricalRangeArtifactStore,
        capture_writer: RetrospectiveCaptureWriter,
        dataset_builder: RetrospectiveDatasetBuilder,
        snapshot_writer: RetrospectiveSnapshotWriter,
        producer_code_hash: str,
        input_loader: HistoricalRangeBridgeInputLoader | None = None,
    ) -> None:
        self._artifact_store = artifact_store
        self._capture_writer = capture_writer
        self._dataset_builder = dataset_builder
        self._snapshot_writer = snapshot_writer
        self._producer_code_hash = require_sha256(producer_code_hash, field_name="producer_code_hash")
        self._input_loader = input_loader

    def build(
        self,
        *,
        operation_id: str,
        request: HistoricalRangeDatasetBridgeRequestV1,
        candidates: Iterable[HistoricalRangeBridgeCandidateV1] | None = None,
        resolved_request_hash: str,
        heartbeat: Callable[[str], None] | None = None,
    ) -> tuple[HistoricalRangeDatasetBridgeReceiptV1, HistoricalRangeArtifactRefV1]:
        if request.retrospective_selector_policy_hash != RETROSPECTIVE_SELECTOR_POLICY_HASH:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "bridge request uses a non-retrospective selector policy hash",
            )
        if self._input_loader is not None:
            if candidates is not None:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "production bridge input must come from exact request refs",
                )
            candidates = self._input_loader.load(request=request)
        elif candidates is None:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "bridge has no exact-ref input loader",
            )
        if heartbeat is not None:
            heartbeat("INPUT_RESOLVED")
        exact_candidate_hashes = {item.semantic_content_hash for item in request.candidate_refs}
        exact_outcome_hashes = {item.semantic_content_hash for item in request.outcome_refs}
        grouped: dict[str, list[HistoricalRangeBridgeCandidateV1]] = {}
        lineage_count = 0
        for candidate in candidates:
            if (
                candidate.candidate_artifact_ref.semantic_content_hash not in exact_candidate_hashes
                or candidate.outcome_ref.semantic_content_hash not in exact_outcome_hashes
                or candidate.lineage.range_run_id not in request.range_run_ids
            ):
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "bridge input contains evidence outside the exact request",
                )
            lineage_count += 1
            variants = grouped.setdefault(candidate.canonical_signal_id, [])
            if variants:
                existing = variants[0]
                if (
                    existing.stage_payload_hash != candidate.stage_payload_hash
                    or _economic_calculation_set_hash(existing)
                    != _economic_calculation_set_hash(candidate)
                ):
                    raise HistoricalRangeDatasetBridgeError(
                        REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                        "duplicate canonical signal has conflicting observation or label content",
                    )
            variants.append(candidate)
        policies = self._load_policy_bundles(request) if grouped else {}
        for candidates_for_signal in grouped.values():
            for candidate in candidates_for_signal:
                if candidate.outcome.historical_range_policy_bundle_hash not in policies:
                    raise HistoricalRangeDatasetBridgeError(
                        REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                        "bridge outcome policy ref is outside the exact request",
                    )
                try:
                    outcome_envelope = self._artifact_store.load(candidate.outcome_ref)
                except Exception as exc:
                    raise HistoricalRangeDatasetBridgeError(
                        REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                        "bridge outcome artifact readback failed",
                    ) from exc
                if outcome_envelope.payload != candidate.outcome.outcome_json:
                    raise HistoricalRangeDatasetBridgeError(
                        REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                        "durable outcome fact and outcome artifact payload differ",
                    )
        observations, labels = _project(
            tuple(tuple(sorted(grouped[key], key=lambda item: item.outcome_ref.semantic_content_hash)) for key in sorted(grouped)),
            policies=policies,
            requested_maturity_statuses=request.requested_maturity_statuses,
        )
        if not observations:
            bridge_ref = self._publish_bridge_artifact(
                request=request,
                resolved_request_hash=resolved_request_hash,
                operation_id=operation_id,
                status=HistoricalRangeBridgeResultStatus.VALID_EMPTY,
                observations=(),
                labels=(),
                capture_ids=(),
                build_id=None,
                snapshot_id=None,
            )
            receipt = HistoricalRangeDatasetBridgeReceiptV1(
                operation_id=operation_id,
                request_hash=str(request.request_hash),
                result_status=HistoricalRangeBridgeResultStatus.VALID_EMPTY,
                observation_count=0,
                label_count=0,
                canonical_signal_count=0,
                range_lineage_count=0,
                retrospective_selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
                bridge_artifact_ref=bridge_ref,
                reason_codes=(REASON_DATASET_BRIDGE_VALID_EMPTY,),
            )
            return receipt, self._publish_receipt(
                receipt=receipt,
                resolved_request_hash=resolved_request_hash,
                upstream_refs=(bridge_ref,),
            )
        capture_ids = self._capture_writer.capture(
            request=request,
            observations=observations,
            labels=labels,
        )
        if heartbeat is not None:
            heartbeat("CAPTURED")
        resolve_labels = getattr(self._capture_writer, "resolve_persisted_labels", None)
        if resolve_labels is not None:
            labels = tuple(resolve_labels(labels))
        if not capture_ids:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "non-empty bridge did not create capture identities",
            )
        build_id = self._dataset_builder.build(
            request=request,
            capture_ids=tuple(sorted(set(capture_ids))),
            observations=observations,
            labels=labels,
        )
        if heartbeat is not None:
            heartbeat("BUILT")
        snapshot_id, selector_hash = self._snapshot_writer.seal(
            build_id=build_id,
            expected_selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
        )
        if heartbeat is not None:
            heartbeat("SEALED")
        if selector_hash != RETROSPECTIVE_SELECTOR_POLICY_HASH:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "snapshot writer returned a mixed or formal selector hash",
            )
        bridge_ref = self._publish_bridge_artifact(
            request=request,
            resolved_request_hash=resolved_request_hash,
            operation_id=operation_id,
            status=HistoricalRangeBridgeResultStatus.SEALED,
            observations=observations,
            labels=labels,
            capture_ids=tuple(sorted(set(capture_ids))),
            build_id=build_id,
            snapshot_id=snapshot_id,
        )
        receipt = HistoricalRangeDatasetBridgeReceiptV1(
            operation_id=operation_id,
            request_hash=str(request.request_hash),
            result_status=HistoricalRangeBridgeResultStatus.SEALED,
            observation_count=len(observations),
            label_count=len(labels),
            canonical_signal_count=len(observations),
            range_lineage_count=lineage_count,
            retrospective_selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
            dataset_build_id=build_id,
            sealed_snapshot_id=snapshot_id,
            bridge_artifact_ref=bridge_ref,
        )
        return receipt, self._publish_receipt(
            receipt=receipt,
            resolved_request_hash=resolved_request_hash,
            upstream_refs=(bridge_ref,),
        )

    def publish_failed_receipt(
        self,
        *,
        operation_id: str,
        request: HistoricalRangeDatasetBridgeRequestV1,
        resolved_request_hash: str,
        reason_code: str,
        result_status: HistoricalRangeBridgeResultStatus = (
            HistoricalRangeBridgeResultStatus.FAILED
        ),
    ) -> tuple[HistoricalRangeDatasetBridgeReceiptV1, HistoricalRangeArtifactRefV1]:
        if result_status not in {
            HistoricalRangeBridgeResultStatus.FAILED,
            HistoricalRangeBridgeResultStatus.RETRYABLE_FAILED,
        }:
            raise ValueError("failure receipt requires FAILED or RETRYABLE_FAILED status")
        bridge_ref = self._publish_bridge_artifact(
            request=request,
            resolved_request_hash=resolved_request_hash,
            operation_id=operation_id,
            status=result_status,
            observations=(),
            labels=(),
            capture_ids=(),
            build_id=None,
            snapshot_id=None,
        )
        receipt = HistoricalRangeDatasetBridgeReceiptV1(
            operation_id=operation_id,
            request_hash=str(request.request_hash),
            result_status=result_status,
            observation_count=0,
            label_count=0,
            canonical_signal_count=0,
            range_lineage_count=0,
            retrospective_selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
            bridge_artifact_ref=bridge_ref,
            reason_codes=(reason_code,),
        )
        return receipt, self._publish_receipt(
            receipt=receipt,
            resolved_request_hash=resolved_request_hash,
            upstream_refs=(bridge_ref,),
        )

    def _load_policy_bundles(
        self, request: HistoricalRangeDatasetBridgeRequestV1
    ) -> dict[
        str,
        tuple[HistoricalRangeArtifactRefV1, HistoricalRangeOutcomePolicyBundleV1, str],
    ]:
        loaded: dict[
            str,
            tuple[HistoricalRangeArtifactRefV1, HistoricalRangeOutcomePolicyBundleV1, str],
        ] = {}
        for ref in request.policy_bundle_refs:
            if ref.artifact_kind is not HistoricalRangeArtifactKind.REQUEST:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "historical-range policy ref/hash closure is invalid",
                )
            try:
                envelope = self._artifact_store.load(ref)
                policy = HistoricalRangeOutcomePolicyBundleV1.model_validate(
                    envelope.payload
                )
            except Exception as exc:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "historical-range policy bundle readback is invalid",
                ) from exc
            if policy.policy_bundle_hash != ref.payload_sha256:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "historical-range policy payload hash differs from artifact ref",
                )
            expected_components = {
                item.component_role: item.component_hash
                for item in policy.components
            }
            if request.policy_component_hashes[ref.payload_sha256] != expected_components:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "bridge policy component hashes differ from the frozen bundle",
                )
            component_set_hash = canonical_json_sha256(
                [
                    {
                        "component_role": role,
                        "component_hash": request.policy_component_hashes[
                            ref.payload_sha256
                        ][role],
                    }
                    for role in sorted(
                        request.policy_component_hashes[ref.payload_sha256]
                    )
                ]
            )
            loaded[ref.payload_sha256] = (ref, policy, component_set_hash)
        return loaded

    def _publish_bridge_artifact(
        self,
        *,
        request: HistoricalRangeDatasetBridgeRequestV1,
        resolved_request_hash: str,
        operation_id: str,
        status: HistoricalRangeBridgeResultStatus,
        observations: tuple[HistoricalRangeBridgeObservationV1, ...],
        labels: tuple[HistoricalRangeBridgeLabelV1, ...],
        capture_ids: tuple[str, ...],
        build_id: str | None,
        snapshot_id: str | None,
    ) -> HistoricalRangeArtifactRefV1:
        artifact = HistoricalRangeDatasetBridgeArtifactV1(
            operation_id=operation_id,
            request=request,
            request_hash=str(request.request_hash),
            result_status=status,
            observations=observations,
            labels=labels,
            capture_ids=capture_ids,
            build_id=build_id,
            sealed_snapshot_id=snapshot_id,
            selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
            capabilities=(
                ("RESEARCH_AUDIT", "INTERNAL_BOOTSTRAP")
                if observations
                else ()
            ),
            producer_code_hash=self._producer_code_hash,
        )
        payload = artifact.model_dump(mode="json")
        upstream = tuple(
            sorted(
                (
                    *request.successful_day_refs,
                    *request.candidate_refs,
                    *request.outcome_refs,
                    *request.summary_refs,
                    *request.policy_bundle_refs,
                ),
                key=lambda item: (
                    item.artifact_kind.value,
                    item.semantic_content_hash,
                    item.relative_path,
                ),
            )
        )
        stored = self._artifact_store.publish_payload(
            artifact_kind=HistoricalRangeArtifactKind.DATASET_BRIDGE,
            producer_contract_version="advisory_phase1r_r4_dataset_bridge_v1",
            payload_schema_version="advisory_historical_range_dataset_bridge_artifact_v1",
            resolved_request_hash=resolved_request_hash,
            payload=payload,
            upstream_refs=upstream,
        )
        envelope = self._artifact_store.load(stored.ref)
        readback = HistoricalRangeDatasetBridgeArtifactV1.model_validate(
            envelope.payload
        )
        if (
            canonical_json_sha256(readback.model_dump(mode="json"))
            != canonical_json_sha256(artifact.model_dump(mode="json"))
            or envelope.upstream_refs != upstream
        ):
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "bridge artifact full readback differs from exact request closure",
            )
        return stored.ref

    def _publish_receipt(
        self,
        *,
        receipt: HistoricalRangeDatasetBridgeReceiptV1,
        resolved_request_hash: str,
        upstream_refs: tuple[HistoricalRangeArtifactRefV1, ...],
    ) -> HistoricalRangeArtifactRefV1:
        return self._artifact_store.publish_payload(
            artifact_kind=HistoricalRangeArtifactKind.DATASET_BRIDGE_RECEIPT,
            producer_contract_version="advisory_phase1r_r4_dataset_bridge_v1",
            payload_schema_version=DATASET_BRIDGE_RECEIPT_SCHEMA_VERSION,
            resolved_request_hash=resolved_request_hash,
            payload=receipt.model_dump(mode="json"),
            upstream_refs=upstream_refs,
        ).ref


class HistoricalRangeDatasetBridgeOperationRepository(Protocol):
    def get_or_create_operation(
        self, request: HistoricalRangeOperationRequestV1
    ) -> tuple[dict[str, Any], bool]: ...

    def transition_operation(self, **kwargs: Any) -> dict[str, Any]: ...


class HistoricalRangeDatasetBridgeApplicationService:
    """Own the durable BUILD_DATASET_BRIDGE lease and terminal receipt."""

    def __init__(
        self,
        *,
        repository: HistoricalRangeDatasetBridgeOperationRepository,
        artifact_store: HistoricalRangeArtifactStore,
        bridge_service: HistoricalRangeDatasetBridgeService,
    ) -> None:
        self._repository = repository
        self._artifact_store = artifact_store
        self._bridge_service = bridge_service

    def build_until_stable_boundary(
        self,
        *,
        request: HistoricalRangeDatasetBridgeRequestV1,
        resolved_request_hash: str,
        worker_id: str,
    ) -> tuple[HistoricalRangeDatasetBridgeReceiptV1, HistoricalRangeArtifactRefV1]:
        resolved_request_hash = require_sha256(
            resolved_request_hash,
            field_name="resolved_request_hash",
        )
        operation_request = HistoricalRangeOperationRequestV1(
            operation_id=derive_prefixed_id(
                "ahrop",
                {
                    "batch_id": request.batch_id,
                    "operation_type": HistoricalRangeOperationType.BUILD_DATASET_BRIDGE.value,
                    "idempotency_key": request.operation_idempotency_key,
                },
            ),
            batch_id=request.batch_id,
            operation_type=HistoricalRangeOperationType.BUILD_DATASET_BRIDGE,
            operation_idempotency_key=request.operation_idempotency_key,
            request_payload_sha256=str(request.request_hash),
            expected_row_version=request.expected_batch_row_version,
        )
        operation, idempotent = self._repository.get_or_create_operation(
            operation_request
        )
        if idempotent and str(operation.get("status")) in {
            HistoricalRangeOperationStatus.COMPLETED.value,
            HistoricalRangeOperationStatus.FAILED.value,
        }:
            return self._load_terminal_receipt(
                operation=operation,
                request=request,
            )
        operation = self._claim(
            operation=operation,
            request=request,
            resolved_request_hash=resolved_request_hash,
            worker_id=worker_id,
        )
        attempt_no = int(operation["attempt_no"])
        fencing_token = int(operation["fencing_token"])
        lease_token = str(operation["lease_token"])
        started_at = datetime.now(UTC)
        current = {"operation": operation}

        def heartbeat(phase: str) -> None:
            row = current["operation"]
            current["operation"] = self._repository.transition_operation(
                operation_id=operation_request.operation_id,
                expected_row_version=int(row["row_version"]),
                target_status=HistoricalRangeOperationStatus.RUNNING,
                attempt_no=attempt_no,
                worker_id=worker_id,
                lease_token=lease_token,
                lease_expires_at=datetime.now(UTC)
                + timedelta(seconds=request.lease_seconds),
                fencing_token=fencing_token,
                stable_keyset_cursor_json={"phase": phase},
            )

        try:
            receipt, receipt_ref = self._bridge_service.build(
                operation_id=operation_request.operation_id,
                request=request,
                resolved_request_hash=resolved_request_hash,
                heartbeat=heartbeat,
            )
            return self._finish(
                operation=current["operation"],
                request=request,
                receipt=receipt,
                receipt_ref=receipt_ref,
                worker_id=worker_id,
                lease_token=lease_token,
                fencing_token=fencing_token,
                attempt_no=attempt_no,
                started_at=started_at,
                target_status=HistoricalRangeOperationStatus.COMPLETED,
                error_json=None,
            )
        except HistoricalRangeDatasetBridgeError as error:
            return self._finish_failure(
                operation=current["operation"],
                request=request,
                resolved_request_hash=resolved_request_hash,
                worker_id=worker_id,
                lease_token=lease_token,
                fencing_token=fencing_token,
                attempt_no=attempt_no,
                started_at=started_at,
                target_status=HistoricalRangeOperationStatus.FAILED,
                reason_code=error.reason_code,
                error_type=type(error).__name__,
            )
        except HistoricalRangeContractError as error:
            target = (
                HistoricalRangeOperationStatus.RETRYABLE_FAILED
                if error.reason_code
                in {
                    REASON_DATABASE_CAPACITY_EXHAUSTED,
                    REASON_DATABASE_UNAVAILABLE,
                }
                else HistoricalRangeOperationStatus.FAILED
            )
            return self._finish_failure(
                operation=current["operation"],
                request=request,
                resolved_request_hash=resolved_request_hash,
                worker_id=worker_id,
                lease_token=lease_token,
                fencing_token=fencing_token,
                attempt_no=attempt_no,
                started_at=started_at,
                target_status=target,
                reason_code=error.reason_code,
                error_type=type(error).__name__,
            )
        except (
            psycopg2.OperationalError,
            psycopg2.InterfaceError,
            psycopg2.errors.SerializationFailure,
            psycopg2.errors.DeadlockDetected,
            psycopg2.errors.LockNotAvailable,
        ):
            return self._finish_failure(
                operation=current["operation"],
                request=request,
                resolved_request_hash=resolved_request_hash,
                worker_id=worker_id,
                lease_token=lease_token,
                fencing_token=fencing_token,
                attempt_no=attempt_no,
                started_at=started_at,
                target_status=HistoricalRangeOperationStatus.RETRYABLE_FAILED,
                reason_code=REASON_DATABASE_UNAVAILABLE,
                error_type="DatabaseUnavailable",
            )
        except Exception as error:
            logger.exception(
                "historical_range_dataset_bridge_failed operation_id=%s",
                operation_request.operation_id,
            )
            return self._finish_failure(
                operation=current["operation"],
                request=request,
                resolved_request_hash=resolved_request_hash,
                worker_id=worker_id,
                lease_token=lease_token,
                fencing_token=fencing_token,
                attempt_no=attempt_no,
                started_at=started_at,
                target_status=HistoricalRangeOperationStatus.FAILED,
                reason_code=REASON_DATASET_BRIDGE_FAILED,
                error_type=type(error).__name__,
            )

    def _claim(
        self,
        *,
        operation: dict[str, Any],
        request: HistoricalRangeDatasetBridgeRequestV1,
        resolved_request_hash: str,
        worker_id: str,
    ) -> dict[str, Any]:
        expired_attempt = None
        if str(operation["status"]) == HistoricalRangeOperationStatus.RUNNING.value:
            lease_expires_at = operation.get("lease_expires_at")
            if lease_expires_at is None or lease_expires_at > datetime.now(UTC):
                raise HistoricalRangeDatasetBridgeError(
                    REASON_REPOSITORY_CONFLICT,
                    "BUILD_DATASET_BRIDGE operation already has an active lease",
                )
            _, expired_ref = self._bridge_service.publish_failed_receipt(
                operation_id=str(operation["operation_id"]),
                request=request,
                resolved_request_hash=resolved_request_hash,
                reason_code=REASON_DATASET_BRIDGE_FAILED,
                result_status=HistoricalRangeBridgeResultStatus.RETRYABLE_FAILED,
            )
            expired_at = datetime.now(UTC)
            expired_attempt = HistoricalRangeOperationAttemptV1(
                attempt_id=derive_prefixed_id(
                    "ahroba",
                    {
                        "operation_id": operation["operation_id"],
                        "attempt_no": operation["attempt_no"],
                        "fencing_token": operation["fencing_token"],
                    },
                ),
                operation_id=str(operation["operation_id"]),
                attempt_no=int(operation["attempt_no"]),
                worker_id=str(operation["worker_id"]),
                lease_token=str(operation["lease_token"]),
                fencing_token=int(operation["fencing_token"]),
                status=HistoricalRangeOperationStatus.RETRYABLE_FAILED.value,
                input_cursor_json=operation.get("stable_keyset_cursor_json"),
                result_cursor_json={"phase": "LEASE_EXPIRED"},
                input_hash=str(request.request_hash),
                result_hash=expired_ref.semantic_content_hash,
                attempt_receipt_ref=expired_ref,
                reason_codes=(REASON_DATASET_BRIDGE_FAILED,),
                error_json={
                    "reason_code": REASON_DATASET_BRIDGE_FAILED,
                    "error_type": "LeaseExpired",
                },
                started_at=operation.get("started_at") or expired_at,
                finished_at=expired_at,
            )
        return self._repository.transition_operation(
            operation_id=str(operation["operation_id"]),
            expected_row_version=int(operation["row_version"]),
            target_status=HistoricalRangeOperationStatus.RUNNING,
            attempt_no=int(operation["attempt_no"]) + 1,
            worker_id=worker_id,
            lease_token=uuid4().hex,
            lease_expires_at=datetime.now(UTC)
            + timedelta(seconds=request.lease_seconds),
            fencing_token=int(operation.get("fencing_token") or 0) + 1,
            stable_keyset_cursor_json=operation.get("stable_keyset_cursor_json"),
            started_at=datetime.now(UTC),
            expired_attempt=expired_attempt,
        )

    def _finish_failure(
        self,
        *,
        operation: dict[str, Any],
        request: HistoricalRangeDatasetBridgeRequestV1,
        resolved_request_hash: str,
        worker_id: str,
        lease_token: str,
        fencing_token: int,
        attempt_no: int,
        started_at: datetime,
        target_status: HistoricalRangeOperationStatus,
        reason_code: str,
        error_type: str,
    ) -> tuple[HistoricalRangeDatasetBridgeReceiptV1, HistoricalRangeArtifactRefV1]:
        receipt, receipt_ref = self._bridge_service.publish_failed_receipt(
            operation_id=str(operation["operation_id"]),
            request=request,
            resolved_request_hash=resolved_request_hash,
            reason_code=reason_code,
            result_status=(
                HistoricalRangeBridgeResultStatus.RETRYABLE_FAILED
                if target_status
                is HistoricalRangeOperationStatus.RETRYABLE_FAILED
                else HistoricalRangeBridgeResultStatus.FAILED
            ),
        )
        return self._finish(
            operation=operation,
            request=request,
            receipt=receipt,
            receipt_ref=receipt_ref,
            worker_id=worker_id,
            lease_token=lease_token,
            fencing_token=fencing_token,
            attempt_no=attempt_no,
            started_at=started_at,
            target_status=target_status,
            error_json={"reason_code": reason_code, "error_type": error_type},
        )

    def _finish(
        self,
        *,
        operation: dict[str, Any],
        request: HistoricalRangeDatasetBridgeRequestV1,
        receipt: HistoricalRangeDatasetBridgeReceiptV1,
        receipt_ref: HistoricalRangeArtifactRefV1,
        worker_id: str,
        lease_token: str,
        fencing_token: int,
        attempt_no: int,
        started_at: datetime,
        target_status: HistoricalRangeOperationStatus,
        error_json: dict[str, Any] | None,
    ) -> tuple[HistoricalRangeDatasetBridgeReceiptV1, HistoricalRangeArtifactRefV1]:
        finished_at = datetime.now(UTC)
        cursor = {
            "phase": target_status.value,
            "dataset_build_id": receipt.dataset_build_id,
            "sealed_snapshot_id": receipt.sealed_snapshot_id,
        }
        attempt = HistoricalRangeOperationAttemptV1(
            attempt_id=derive_prefixed_id(
                "ahroba",
                {
                    "operation_id": receipt.operation_id,
                    "attempt_no": attempt_no,
                    "fencing_token": fencing_token,
                },
            ),
            operation_id=receipt.operation_id,
            attempt_no=attempt_no,
            worker_id=worker_id,
            lease_token=lease_token,
            fencing_token=fencing_token,
            status=target_status.value,
            input_cursor_json=operation.get("stable_keyset_cursor_json"),
            result_cursor_json=cursor,
            input_hash=str(request.request_hash),
            result_hash=receipt_ref.semantic_content_hash,
            attempt_receipt_ref=receipt_ref,
            reason_codes=receipt.reason_codes,
            error_json=error_json,
            started_at=started_at,
            finished_at=finished_at,
        )
        terminal = target_status in {
            HistoricalRangeOperationStatus.COMPLETED,
            HistoricalRangeOperationStatus.FAILED,
        }
        self._repository.transition_operation(
            operation_id=receipt.operation_id,
            expected_row_version=int(operation["row_version"]),
            target_status=target_status,
            attempt_no=attempt_no,
            fencing_token=fencing_token,
            stable_keyset_cursor_json=cursor,
            result_status=receipt.result_status.value if terminal else None,
            result_ref=receipt_ref if terminal else None,
            error_json=error_json,
            finished_at=finished_at if terminal else None,
            attempt=attempt,
        )
        return receipt, receipt_ref

    def _load_terminal_receipt(
        self,
        *,
        operation: dict[str, Any],
        request: HistoricalRangeDatasetBridgeRequestV1,
    ) -> tuple[HistoricalRangeDatasetBridgeReceiptV1, HistoricalRangeArtifactRefV1]:
        raw_ref = operation.get("result_ref")
        if raw_ref is None:
            raise HistoricalRangeDatasetBridgeError(
                REASON_REPOSITORY_CONFLICT,
                "terminal BUILD_DATASET_BRIDGE operation has no result receipt",
            )
        ref = HistoricalRangeArtifactRefV1.model_validate(raw_ref)
        if ref.artifact_kind is not HistoricalRangeArtifactKind.DATASET_BRIDGE_RECEIPT:
            raise HistoricalRangeDatasetBridgeError(
                REASON_REPOSITORY_CONFLICT,
                "terminal BUILD_DATASET_BRIDGE result kind is invalid",
            )
        receipt = HistoricalRangeDatasetBridgeReceiptV1.model_validate(
            self._artifact_store.load(ref).payload
        )
        if (
            receipt.operation_id != str(operation["operation_id"])
            or receipt.request_hash != request.request_hash
            or receipt.result_status.value != str(operation["result_status"])
        ):
            raise HistoricalRangeDatasetBridgeError(
                REASON_REPOSITORY_CONFLICT,
                "terminal BUILD_DATASET_BRIDGE receipt differs from operation state",
            )
        return receipt, ref


def _project(
    candidate_groups: tuple[tuple[HistoricalRangeBridgeCandidateV1, ...], ...],
    *,
    policies: dict[
        str,
        tuple[HistoricalRangeArtifactRefV1, HistoricalRangeOutcomePolicyBundleV1, str],
    ],
    requested_maturity_statuses: tuple[HistoricalRangeOutcomeStatus, ...],
) -> tuple[tuple[HistoricalRangeBridgeObservationV1, ...], tuple[HistoricalRangeBridgeLabelV1, ...]]:
    observations: list[HistoricalRangeBridgeObservationV1] = []
    labels: list[HistoricalRangeBridgeLabelV1] = []
    for variants in candidate_groups:
        if not variants:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "bridge projection received an empty signal group",
            )
        candidate = variants[0]
        rows = materialize_retrospective_observation_row_bundle(
            plan=candidate.capture_plan,
            stage_payload=candidate.stage_payload,
            candidate_fact=candidate.candidate_fact,
            created_by_capture_batch_id="bridge_projection_preflight",
        )
        observation_hash = str(
            rows.observation_version["observation_content_hash"]
        )
        observation_id = str(rows.observation_version["observation_version_id"])
        selection_stage_id = str(
            next(
                item["stage_evidence_id"]
                for item in rows.stage_evidence_rows
                if item["stage"] == "selection_effective"
            )
        )
        if (
            candidate.owner.observation_version_id != observation_id
            or candidate.owner.candidate_stage_evidence_id != selection_stage_id
        ):
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "outcome owner differs from exact retrospective observation materialization",
            )
        for variant in variants[1:]:
            variant_rows = materialize_retrospective_observation_row_bundle(
                plan=variant.capture_plan,
                stage_payload=variant.stage_payload,
                candidate_fact=variant.candidate_fact,
                created_by_capture_batch_id="bridge_projection_preflight",
            )
            if (
                variant_rows.observation_version["observation_version_id"] != observation_id
                or variant.owner.observation_version_id != observation_id
                or variant.owner.candidate_stage_evidence_id != selection_stage_id
            ):
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "duplicate range lineage changes the economic observation identity",
                )
        accepted_outcome_refs = tuple(
            sorted(
                {item.outcome_ref.semantic_content_hash: item.outcome_ref for item in variants}.values(),
                key=lambda ref: ref.semantic_content_hash,
            )
        )
        observation_payload = retrospective_observation_payload(
            plan=candidate.capture_plan,
            candidate_fact=candidate.candidate_fact,
            stage_evidence_bundle_hash=canonical_json_sha256(
                [item["content_hash"] for item in rows.stage_evidence_rows]
            ),
        )
        observation = HistoricalRangeBridgeObservationV1(
            canonical_signal_id=candidate.canonical_signal_id,
            observation_version_id=observation_id,
            observation_content_hash=observation_hash,
            lineage=candidate.lineage,
            capture_plan=candidate.capture_plan,
            candidate_fact=candidate.candidate_fact,
            owner=candidate.owner,
            observation_payload=observation_payload,
            stage_payload=candidate.stage_payload,
            lineage_variants=tuple(item.lineage for item in variants),
            capture_plan_variants=tuple(item.capture_plan for item in variants),
            accepted_outcome_refs=accepted_outcome_refs,
        )
        artifact = HistoricalRangeOutcomeArtifactV2.model_validate(
            candidate.outcome.outcome_json
        )
        policy_ref, policy, component_set_hash = policies[
            candidate.outcome.historical_range_policy_bundle_hash
        ]
        executable_results = list(
            _eligible_executable_results(
                artifact.model_dump(mode="python"),
                requested_maturity_statuses=requested_maturity_statuses,
            )
        )
        for result in executable_results:
            if result.owner != candidate.owner:
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "outcome calculation owner differs from exact retrospective observation",
                )
            if result.owner.owner_type.value != "CANDIDATE":
                continue
            if (
                result.horizon_trading_days not in policy.horizons
                or result.projection.value
                not in policy.projections_by_horizon[result.horizon_trading_days]
            ):
                raise HistoricalRangeDatasetBridgeError(
                    REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                    "outcome projection lies outside the frozen policy bundle",
                )
        if not executable_results:
            raise HistoricalRangeDatasetBridgeError(
                REASON_DATASET_BRIDGE_LINEAGE_CONFLICT,
                "candidate executable outcome has no fine-grained Phase 1 projections",
            )
        for result in executable_results:
            label_payload = {
                "schema_version": "advisory_phase1_retrospective_label_v2",
                "canonical_signal_id": candidate.canonical_signal_id,
                "observation_version_id": observation_id,
                "symbol": candidate.symbol,
                "projection": result.projection.value,
                "horizon_trade_days": result.horizon_trading_days,
                "outcome_version_id": candidate.outcome.outcome_version_id,
                "outcome_content_hash": candidate.outcome.outcome_content_hash,
                "historical_range_policy_bundle_ref": policy_ref.model_dump(mode="json"),
                "historical_range_policy_bundle_hash": candidate.outcome.historical_range_policy_bundle_hash,
                "policy_component_set_hash": component_set_hash,
                "calculation_evidence_hash": result.calculation_evidence.evidence_hash,
                "evidence_scope": "RETROSPECTIVE_RESEARCH_ONLY",
            }
            label_hash = canonical_json_sha256(label_payload)
            labels.append(
                HistoricalRangeBridgeLabelV1(
                    canonical_signal_id=candidate.canonical_signal_id,
                    label_version_id=derive_prefixed_id("olbv", label_payload),
                    label_content_hash=label_hash,
                    observation_version_id=observation_id,
                    symbol=candidate.symbol,
                    projection=result.projection,
                    horizon_trade_days=result.horizon_trading_days,
                    outcome_version_id=candidate.outcome.outcome_version_id,
                    outcome_content_hash=str(candidate.outcome.outcome_content_hash),
                    outcome_ref=candidate.outcome_ref,
                    label_as_of_trade_date=candidate.outcome.label_as_of_trade_date,
                    accepted_outcome_refs=accepted_outcome_refs,
                    historical_range_policy_bundle_hash=candidate.outcome.historical_range_policy_bundle_hash,
                    historical_range_policy_bundle_ref=policy_ref,
                    policy_component_set_hash=component_set_hash,
                    outcome_result=result,
                    calculation_evidence=result.calculation_evidence,
                )
            )
        observations.append(observation)
    return tuple(observations), tuple(labels)


_BRIDGE_EXECUTABLE_PROJECTIONS = {
    Projection.RETURN_GROSS,
    Projection.RETURN_NET_ABSOLUTE,
    Projection.RETURN_NET_EXCESS,
    Projection.EXECUTABLE_MFE,
    Projection.EXECUTABLE_MAE,
}


def _eligible_executable_results(
    outcome_json: Mapping[str, Any],
    *,
    requested_maturity_statuses: tuple[HistoricalRangeOutcomeStatus, ...],
) -> tuple[OutcomeCalculationResult, ...]:
    artifact = HistoricalRangeOutcomeArtifactV2.model_validate(outcome_json)
    eligible: list[OutcomeCalculationResult] = []
    for raw_result in artifact.calculation_results:
        result = OutcomeCalculationResult.model_validate(raw_result)
        if (
            result.projection in _BRIDGE_EXECUTABLE_PROJECTIONS
            and map_historical_range_maturity(result)
            in requested_maturity_statuses
        ):
            eligible.append(result)
    return tuple(eligible)


def _economic_calculation_set_hash(candidate: HistoricalRangeBridgeCandidateV1) -> str:
    artifact = HistoricalRangeOutcomeArtifactV2.model_validate(candidate.outcome.outcome_json)
    payloads = []
    for raw_result in artifact.calculation_results:
        result = OutcomeCalculationResult.model_validate(raw_result)
        payloads.append(
            result.model_dump(
                mode="json",
                exclude={"owner", "projection_payload_hash", "calculation_evidence"},
            )
        )
    return canonical_json_sha256(payloads)
