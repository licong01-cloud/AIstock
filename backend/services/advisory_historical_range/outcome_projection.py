"""Pure Phase 1R outcome projections over the shared Phase 1 valuation core."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Iterable, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_historical_range.canonical import (
    canonical_json_sha256,
    canonicalize,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeEvaluationWindowType,
    HistoricalRangeOutcomeArtifactV2,
    HistoricalRangeOutcomeFactV1,
    HistoricalRangeOutcomeProjection,
    HistoricalRangeOutcomeRevisionReason,
    HistoricalRangeOutcomeStatus,
    HistoricalRangeOutcomeWorkItemV1,
    derive_prefixed_id,
    require_sha256,
)
from backend.services.advisory_phase1.label_policy import Projection
from backend.services.advisory_phase1.outcome_engine import (
    EntryStatus,
    MaturityStatus,
    OutcomeCalculationRequest,
    OutcomeCalculationResult,
    OutcomeEventStatus,
    PositionPathValuationCore,
)


_RECOMMENDATION_PROJECTIONS = {
    Projection.RETURN_GROSS,
    Projection.RETURN_NET_ABSOLUTE,
    Projection.RETURN_NET_EXCESS,
    Projection.PATH_MFE,
    Projection.PATH_MAE,
}
_EXECUTABLE_PROJECTIONS = {
    Projection.RETURN_GROSS,
    Projection.RETURN_NET_ABSOLUTE,
    Projection.RETURN_NET_EXCESS,
    Projection.EXECUTABLE_MFE,
    Projection.EXECUTABLE_MAE,
}


class HistoricalRangeProjectionError(ValueError):
    pass


class HistoricalRangeAggregateCalculationResultV1(BaseModel):
    """One list/range cohort projection derived only from child outcomes."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    calculation_kind: Literal["AGGREGATE_COHORT"] = "AGGREGATE_COHORT"
    projection: Projection
    horizon_trading_days: int = Field(ge=0)
    decision_trade_date: date
    maturity_status: HistoricalRangeOutcomeStatus
    projection_value_decimal: Decimal | None = None
    next_refresh_trade_date: date | None = None
    child_outcome_refs: tuple[HistoricalRangeArtifactRefV1, ...]
    maturity_coverage: dict[str, int]
    reason_codes: tuple[str, ...] = ()
    maturity_coverage_hash: str | None = Field(default=None, min_length=64, max_length=64)
    calculation_evidence_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("maturity_coverage_hash", "calculation_evidence_hash")
    @classmethod
    def _hash(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _closure(self) -> "HistoricalRangeAggregateCalculationResultV1":
        refs = tuple(sorted(self.child_outcome_refs, key=lambda item: item.semantic_content_hash))
        if len(refs) != len({item.semantic_content_hash for item in refs}):
            raise ValueError("aggregate child outcome refs must be unique")
        if self.maturity_coverage.get("eligible_total") != len(refs):
            raise ValueError("aggregate maturity coverage differs from child outcome set")
        terminal = self.maturity_status in {
            HistoricalRangeOutcomeStatus.COMPLETE,
            HistoricalRangeOutcomeStatus.TERMINAL,
        }
        if terminal != (self.projection_value_decimal is not None):
            raise ValueError("aggregate numeric value must match complete/terminal maturity")
        if self.maturity_status in {
            HistoricalRangeOutcomeStatus.NOT_DUE,
            HistoricalRangeOutcomeStatus.MATURING,
        }:
            if self.next_refresh_trade_date is None:
                raise ValueError("incomplete aggregate requires next refresh date")
        elif self.next_refresh_trade_date is not None:
            raise ValueError("terminal aggregate cannot retain next refresh date")
        coverage_hash = canonical_json_sha256(self.maturity_coverage)
        if self.maturity_coverage_hash is not None and self.maturity_coverage_hash != coverage_hash:
            raise ValueError("aggregate maturity coverage hash differs")
        evidence_hash = canonical_json_sha256(
            {
                "projection": self.projection.value,
                "horizon_trading_days": self.horizon_trading_days,
                "decision_trade_date": self.decision_trade_date,
                "maturity_status": self.maturity_status.value,
                "projection_value_decimal": self.projection_value_decimal,
                "next_refresh_trade_date": self.next_refresh_trade_date,
                "reason_codes": sorted(set(self.reason_codes)),
                "child_outcome_refs": [item.model_dump(mode="json") for item in refs],
                "maturity_coverage_hash": coverage_hash,
            }
        )
        if self.calculation_evidence_hash is not None and self.calculation_evidence_hash != evidence_hash:
            raise ValueError("aggregate calculation evidence hash differs")
        object.__setattr__(self, "child_outcome_refs", refs)
        object.__setattr__(self, "reason_codes", tuple(sorted(set(self.reason_codes))))
        object.__setattr__(self, "maturity_coverage_hash", coverage_hash)
        object.__setattr__(self, "calculation_evidence_hash", evidence_hash)
        return self


class HistoricalRangeProjectionResultV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    projection_group: HistoricalRangeOutcomeProjection
    evaluation_window_type: HistoricalRangeEvaluationWindowType
    horizon_trade_days: int = Field(ge=0)
    maturity_status: HistoricalRangeOutcomeStatus
    next_refresh_trade_date: date | None = None
    calculation_results: tuple[
        OutcomeCalculationResult | HistoricalRangeAggregateCalculationResultV1, ...
    ] = ()
    calculation_result_set_hash: str | None = Field(default=None, min_length=64, max_length=64)
    reason_codes: tuple[str, ...] = ()

    @field_validator("calculation_result_set_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return require_sha256(value, field_name="calculation_result_set_hash") if value is not None else None

    @model_validator(mode="after")
    def _closure(self) -> "HistoricalRangeProjectionResultV1":
        results = tuple(sorted(self.calculation_results, key=lambda item: item.projection.value))
        if self.maturity_status is HistoricalRangeOutcomeStatus.FAILED:
            if results or not self.reason_codes or self.next_refresh_trade_date is not None:
                raise ValueError("FAILED projection requires reasons, no result, and no refresh date")
            digest = canonical_json_sha256([])
            if self.calculation_result_set_hash is not None and self.calculation_result_set_hash != digest:
                raise ValueError("failed projection result-set hash is invalid")
            object.__setattr__(self, "calculation_result_set_hash", digest)
            object.__setattr__(self, "reason_codes", tuple(sorted(set(self.reason_codes))))
            return self
        if len(results) != len({item.projection for item in results}):
            raise ValueError("projection calculation results must be unique")
        statuses = {map_historical_range_maturity(item) for item in results}
        expected = _combine_maturity(statuses)
        if self.maturity_status is not expected:
            raise ValueError("outer maturity does not match calculation results")
        if expected in {HistoricalRangeOutcomeStatus.NOT_DUE, HistoricalRangeOutcomeStatus.MATURING}:
            scheduled = [
                item.scheduled_maturity_ts.date()
                if isinstance(item, OutcomeCalculationResult)
                else item.next_refresh_trade_date
                for item in results
            ]
            if any(item is None for item in scheduled):
                raise ValueError("incomplete projection result lacks refresh date")
            expected_refresh = min(scheduled)
            if self.next_refresh_trade_date != expected_refresh:
                raise ValueError("incomplete projection requires the earliest exact refresh date")
        elif self.next_refresh_trade_date is not None:
            raise ValueError("terminal projection cannot retain a next refresh date")
        digest = canonical_json_sha256(
            [canonicalize(item.model_dump(mode="python")) for item in results]
        )
        if self.calculation_result_set_hash is not None and self.calculation_result_set_hash != digest:
            raise ValueError("calculation_result_set_hash does not match results")
        reasons = tuple(sorted({reason for result in results for reason in result.reason_codes} | set(self.reason_codes)))
        object.__setattr__(self, "calculation_results", results)
        object.__setattr__(self, "calculation_result_set_hash", digest)
        object.__setattr__(self, "reason_codes", reasons)
        return self


class RecommendationPathOutcomeEngine:
    """Evaluate exact R3 recommendation marks without requiring executable entry."""

    def __init__(self, *, valuation_core: PositionPathValuationCore | None = None) -> None:
        self._valuation_core = valuation_core or PositionPathValuationCore()

    def calculate(
        self,
        *,
        requests: Mapping[Projection, OutcomeCalculationRequest],
        timeline: tuple[date, date, date, date],
        evaluation_window_type: HistoricalRangeEvaluationWindowType,
        horizon_trade_days: int,
    ) -> HistoricalRangeProjectionResultV1:
        if not requests or not set(requests) <= _RECOMMENDATION_PROJECTIONS:
            raise HistoricalRangeProjectionError("recommendation projection set is empty or invalid")
        results = tuple(
            self._valuation_core.calculate(
                requests[projection],
                timeline_override=timeline,
                require_entry_executable=False,
                entry_mark_trade_date=timeline[0],
            )
            for projection in sorted(requests, key=lambda item: item.value)
        )
        return _projection_result(
            group=HistoricalRangeOutcomeProjection.RECOMMENDATION,
            evaluation_window_type=evaluation_window_type,
            horizon_trade_days=horizon_trade_days,
            results=results,
        )


class ExecutablePathOutcomeEngine:
    """Evaluate actual next-open and exit/terminal evidence."""

    def __init__(self, *, valuation_core: PositionPathValuationCore | None = None) -> None:
        self._valuation_core = valuation_core or PositionPathValuationCore()

    def calculate(
        self,
        *,
        requests: Mapping[Projection, OutcomeCalculationRequest],
        timeline: tuple[date, date, date, date] | None,
        evaluation_window_type: HistoricalRangeEvaluationWindowType,
        horizon_trade_days: int,
    ) -> HistoricalRangeProjectionResultV1:
        if not requests or not set(requests) <= _EXECUTABLE_PROJECTIONS:
            raise HistoricalRangeProjectionError("executable projection set is empty or invalid")
        results = tuple(
            self._valuation_core.calculate(
                requests[projection],
                timeline_override=timeline,
                require_entry_executable=True,
            )
            for projection in sorted(requests, key=lambda item: item.value)
        )
        return _projection_result(
            group=HistoricalRangeOutcomeProjection.EXECUTABLE,
            evaluation_window_type=evaluation_window_type,
            horizon_trade_days=horizon_trade_days,
            results=results,
        )


class EpisodeLifecycleOutcomeEngine:
    """Episode window adapter; open episodes remain explicitly right-censored."""

    def __init__(self, *, valuation_core: PositionPathValuationCore | None = None) -> None:
        core = valuation_core or PositionPathValuationCore()
        self._valuation_core = core
        self._recommendation = RecommendationPathOutcomeEngine(valuation_core=core)
        self._executable = ExecutablePathOutcomeEngine(valuation_core=core)

    def calculate(
        self,
        *,
        projection_group: HistoricalRangeOutcomeProjection,
        requests: Mapping[Projection, OutcomeCalculationRequest],
        timeline: tuple[date, date, date, date],
        episode_closed: bool,
    ) -> HistoricalRangeProjectionResultV1:
        early_censor_reason: str | None = None
        if not episode_closed:
            for request in requests.values():
                if request.terminal.disposition.value != "RIGHT_CENSORED":
                    raise HistoricalRangeProjectionError(
                        "open episode requires exact right-censor evidence and cannot synthesize EXIT"
                    )
            if timeline[3] < timeline[2]:
                early_censor_reason = "EPISODE_RIGHT_CENSORED_BEFORE_EARLIEST_SELL"
                if timeline[3] < timeline[1]:
                    calculated = tuple(
                        self._valuation_core.right_censored_before_entry(
                            requests[projection],
                            timeline=timeline,
                            reason_code=early_censor_reason,
                        )
                        for projection in sorted(requests, key=lambda item: item.value)
                    )
                    result = _projection_result(
                        group=projection_group,
                        evaluation_window_type=HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE,
                        horizon_trade_days=0,
                        results=calculated,
                    )
                    return self._normalize_result(
                        result=result,
                        early_censor_reason=early_censor_reason,
                        requests=requests,
                        episode_closed=episode_closed,
                    )
                timeline = (timeline[0], timeline[1], timeline[2], timeline[2])
        else:
            if any(request.terminal.disposition.value == "RIGHT_CENSORED" for request in requests.values()):
                raise HistoricalRangeProjectionError(
                    "closed episode cannot use right-censor evidence"
                )
        engine = self._recommendation if projection_group is HistoricalRangeOutcomeProjection.RECOMMENDATION else self._executable
        result = engine.calculate(
            requests=requests,
            timeline=timeline,
            evaluation_window_type=HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE,
            horizon_trade_days=0,
        )
        return self._normalize_result(
            result=result,
            early_censor_reason=early_censor_reason,
            requests=requests,
            episode_closed=episode_closed,
        )

    @staticmethod
    def _normalize_result(
        *,
        result: HistoricalRangeProjectionResultV1,
        early_censor_reason: str | None,
        requests: Mapping[Projection, OutcomeCalculationRequest],
        episode_closed: bool,
    ) -> HistoricalRangeProjectionResultV1:
        # Episode lifecycle uses the explicit zero sentinel at both outer and
        # fine-grained result levels; the timeline, not a synthetic horizon,
        # carries the actual observed holding interval.
        normalized_items = []
        for item in result.calculation_results:
            payload = {
                **item.model_dump(mode="python"),
                "horizon_trading_days": 0,
                "projection_payload_hash": None,
            }
            if (
                episode_closed
                and item.maturity_status is MaturityStatus.MATURED
                and item.observed_holding_trading_days is None
            ):
                if item.exit_trade_date is None:
                    raise HistoricalRangeProjectionError(
                        "closed matured episode calculation lacks an exit date"
                    )
                payload["observed_holding_trading_days"] = (
                    len(
                        requests[item.projection]
                        .policies.calendar.trading_days_inclusive(
                            item.intended_entry_trade_date,
                            item.exit_trade_date,
                        )
                    )
                    - 1
                )
            normalized_items.append(OutcomeCalculationResult.model_validate(payload))
        normalized = tuple(normalized_items)
        return HistoricalRangeProjectionResultV1(
            projection_group=result.projection_group,
            evaluation_window_type=result.evaluation_window_type,
            horizon_trade_days=0,
            maturity_status=result.maturity_status,
            next_refresh_trade_date=result.next_refresh_trade_date,
            calculation_results=normalized,
            reason_codes=tuple(
                sorted(
                    set(result.reason_codes)
                    | ({early_censor_reason} if early_censor_reason else set())
                )
            ),
        )


class HistoricalRangeOutcomeProjectionBuilder:
    """Close a work item, calculation results, artifact, and append-only fact."""

    def build(
        self,
        *,
        work_item: HistoricalRangeOutcomeWorkItemV1,
        result: HistoricalRangeProjectionResultV1,
        outcome_version: int,
        outcome_artifact_ref: HistoricalRangeArtifactRefV1,
        predecessor: HistoricalRangeOutcomeFactV1 | None = None,
    ) -> tuple[HistoricalRangeOutcomeArtifactV2, HistoricalRangeOutcomeFactV1]:
        artifact = self.build_artifact(
            work_item=work_item,
            result=result,
            outcome_version=outcome_version,
            predecessor=predecessor,
        )
        fact = self.build_fact(
            work_item=work_item,
            result=result,
            artifact=artifact,
            outcome_artifact_ref=outcome_artifact_ref,
            outcome_version=outcome_version,
            predecessor=predecessor,
        )
        return artifact, fact

    def build_artifact(
        self,
        *,
        work_item: HistoricalRangeOutcomeWorkItemV1,
        result: HistoricalRangeProjectionResultV1,
        outcome_version: int,
        predecessor: HistoricalRangeOutcomeFactV1 | None = None,
    ) -> HistoricalRangeOutcomeArtifactV2:
        if result.projection_group is not work_item.projection:
            raise HistoricalRangeProjectionError("work item and result projection groups differ")
        if result.evaluation_window_type is not work_item.evaluation_window_type:
            raise HistoricalRangeProjectionError("work item and result windows differ")
        if result.horizon_trade_days != work_item.horizon_trade_days:
            raise HistoricalRangeProjectionError("work item and result horizons differ")
        if (predecessor is None) != (outcome_version == 1):
            raise HistoricalRangeProjectionError("outcome version/predecessor pair is invalid")
        if predecessor is not None and predecessor.outcome_logical_id != work_item.outcome_logical_id:
            raise HistoricalRangeProjectionError("predecessor belongs to another logical outcome")
        _validate_revision_transition(
            work_item=work_item,
            result=result,
            predecessor=predecessor,
        )
        calculations = tuple(
            canonicalize(item.model_dump(mode="python"))
            for item in result.calculation_results
        )
        upstream_by_hash = {
            item.semantic_content_hash: item
            for item in (
                work_item.subject_ref,
                work_item.policy_bundle_ref,
                *work_item.source_revision_refs,
                *(
                    (work_item.predecessor_outcome_ref,)
                    if work_item.predecessor_outcome_ref is not None
                    else ()
                ),
                *(
                    (work_item.revision_evidence_ref,)
                    if work_item.revision_evidence_ref is not None
                    else ()
                ),
            )
        }
        upstream = tuple(upstream_by_hash[key] for key in sorted(upstream_by_hash))
        version_id = derive_prefixed_id(
            "ahrov",
            {
                "outcome_logical_id": work_item.outcome_logical_id,
                "outcome_version": outcome_version,
                "outcome_input_hash": work_item.outcome_input_hash,
            },
        )
        return HistoricalRangeOutcomeArtifactV2(
            outcome_logical_id=str(work_item.outcome_logical_id),
            outcome_version_id=version_id,
            outcome_input_hash=str(work_item.outcome_input_hash),
            subject_ref=work_item.subject_ref,
            direct_upstream_refs=upstream,
            projection_group=work_item.projection,
            evaluation_window_type=work_item.evaluation_window_type,
            horizon_trade_days=work_item.horizon_trade_days,
            policy_bundle_ref=work_item.policy_bundle_ref,
            policy_bundle_hash=work_item.policy_bundle_hash,
            label_as_of_trade_date=work_item.label_as_of_trade_date,
            source_revision_set_hash=work_item.source_revision_set_hash,
            maturity_status=result.maturity_status,
            next_refresh_trade_date=result.next_refresh_trade_date,
            reason_codes=result.reason_codes,
            calculation_results=calculations,
            calculation_result_set_hash=str(result.calculation_result_set_hash),
            predecessor_outcome_ref=work_item.predecessor_outcome_ref,
            producer_code_hash=work_item.producer_code_hash,
        )

    def build_fact(
        self,
        *,
        work_item: HistoricalRangeOutcomeWorkItemV1,
        result: HistoricalRangeProjectionResultV1,
        artifact: HistoricalRangeOutcomeArtifactV2,
        outcome_artifact_ref: HistoricalRangeArtifactRefV1,
        outcome_version: int,
        predecessor: HistoricalRangeOutcomeFactV1 | None = None,
    ) -> HistoricalRangeOutcomeFactV1:
        if outcome_artifact_ref.artifact_kind is not HistoricalRangeArtifactKind.OUTCOME:
            raise HistoricalRangeProjectionError("outcome_artifact_ref must be OUTCOME")
        if artifact.outcome_input_hash != work_item.outcome_input_hash:
            raise HistoricalRangeProjectionError("outcome artifact differs from work item")
        outcome_json = canonicalize(artifact.model_dump(mode="python"))
        fact = HistoricalRangeOutcomeFactV1(
            outcome_version_id=artifact.outcome_version_id,
            outcome_logical_id=str(work_item.outcome_logical_id),
            outcome_version=outcome_version,
            subject_type=work_item.subject_type,
            subject_id=work_item.subject_id,
            projection=work_item.projection,
            evaluation_window_type=work_item.evaluation_window_type,
            horizon_trade_days=work_item.horizon_trade_days,
            historical_range_policy_bundle_hash=work_item.policy_bundle_hash,
            outcome_input_hash=str(work_item.outcome_input_hash),
            revision_reason=work_item.revision_reason,
            producer_code_hash=work_item.producer_code_hash,
            outcome_contract_version=work_item.outcome_contract_version,
            source_revision_set_hash=work_item.source_revision_set_hash,
            predecessor_outcome_version_id=predecessor.outcome_version_id if predecessor else None,
            predecessor_outcome_hash=predecessor.outcome_content_hash if predecessor else None,
            revision_evidence_ref=work_item.revision_evidence_ref,
            revision_evidence_hash=(
                work_item.revision_evidence_ref.semantic_content_hash if work_item.revision_evidence_ref else None
            ),
            maturity_status=result.maturity_status,
            label_as_of_trade_date=work_item.label_as_of_trade_date,
            next_refresh_trade_date=result.next_refresh_trade_date,
            calculation_evidence_ref=None,
            outcome_artifact_ref=outcome_artifact_ref,
            outcome_json=outcome_json,
        )
        return fact


def _validate_revision_transition(
    *,
    work_item: HistoricalRangeOutcomeWorkItemV1,
    result: HistoricalRangeProjectionResultV1,
    predecessor: HistoricalRangeOutcomeFactV1 | None,
) -> None:
    if predecessor is None:
        if work_item.revision_reason is not HistoricalRangeOutcomeRevisionReason.INITIAL:
            raise HistoricalRangeProjectionError("first outcome version must be INITIAL")
        return
    if work_item.predecessor_outcome_ref != predecessor.outcome_artifact_ref:
        raise HistoricalRangeProjectionError("work item predecessor ref differs from latest outcome")
    if work_item.outcome_input_hash == predecessor.outcome_input_hash:
        raise HistoricalRangeProjectionError("successor outcome must change its input hash")
    if work_item.revision_reason is HistoricalRangeOutcomeRevisionReason.MATURITY_ADVANCE:
        allowed = {
            HistoricalRangeOutcomeStatus.NOT_DUE: {
                HistoricalRangeOutcomeStatus.MATURING,
                HistoricalRangeOutcomeStatus.COMPLETE,
                HistoricalRangeOutcomeStatus.CENSORED,
                HistoricalRangeOutcomeStatus.TERMINAL,
            },
            HistoricalRangeOutcomeStatus.MATURING: {
                HistoricalRangeOutcomeStatus.COMPLETE,
                HistoricalRangeOutcomeStatus.CENSORED,
                HistoricalRangeOutcomeStatus.TERMINAL,
            },
        }
        if result.maturity_status not in allowed.get(predecessor.maturity_status, set()):
            raise HistoricalRangeProjectionError("invalid maturity-advance transition")
        return
    if work_item.revision_reason is HistoricalRangeOutcomeRevisionReason.SOURCE_CORRECTION:
        if work_item.source_revision_set_hash == predecessor.source_revision_set_hash:
            raise HistoricalRangeProjectionError("source correction must change source revision identity")
        return
    if work_item.revision_reason is HistoricalRangeOutcomeRevisionReason.CALCULATION_CORRECTION:
        if (
            work_item.producer_code_hash == predecessor.producer_code_hash
            and work_item.outcome_contract_version == predecessor.outcome_contract_version
        ):
            raise HistoricalRangeProjectionError("calculation correction must change producer identity")
        return
    raise HistoricalRangeProjectionError("successor outcome has an invalid revision reason")


def _projection_result(
    *,
    group: HistoricalRangeOutcomeProjection,
    evaluation_window_type: HistoricalRangeEvaluationWindowType,
    horizon_trade_days: int,
    results: Iterable[OutcomeCalculationResult],
) -> HistoricalRangeProjectionResultV1:
    calculated = tuple(results)
    status = _combine_maturity(
        {map_historical_range_maturity(item) for item in calculated}
    )
    next_refresh = None
    if status in {HistoricalRangeOutcomeStatus.NOT_DUE, HistoricalRangeOutcomeStatus.MATURING}:
        next_refresh = min(item.scheduled_maturity_ts.date() for item in calculated)
    return HistoricalRangeProjectionResultV1(
        projection_group=group,
        evaluation_window_type=evaluation_window_type,
        horizon_trade_days=horizon_trade_days,
        maturity_status=status,
        next_refresh_trade_date=next_refresh,
        calculation_results=calculated,
    )


def map_historical_range_maturity(
    result: OutcomeCalculationResult | HistoricalRangeAggregateCalculationResultV1,
) -> HistoricalRangeOutcomeStatus:
    if isinstance(result, HistoricalRangeAggregateCalculationResultV1):
        return result.maturity_status
    if result.maturity_status is MaturityStatus.PENDING:
        return HistoricalRangeOutcomeStatus.NOT_DUE
    if result.maturity_status is MaturityStatus.MATURED:
        return (
            HistoricalRangeOutcomeStatus.TERMINAL
            if result.outcome_event_status is OutcomeEventStatus.TERMINAL
            else HistoricalRangeOutcomeStatus.COMPLETE
        )
    if result.maturity_status is MaturityStatus.RIGHT_CENSORED:
        return HistoricalRangeOutcomeStatus.CENSORED
    if result.maturity_status is MaturityStatus.UNAVAILABLE:
        if result.entry_status is EntryStatus.NOT_EXECUTABLE or result.missing_source_receipt_hash is not None:
            return HistoricalRangeOutcomeStatus.CENSORED
        return HistoricalRangeOutcomeStatus.MATURING
    raise HistoricalRangeProjectionError(f"unsupported Phase 1 maturity {result.maturity_status}")


def _combine_maturity(statuses: set[HistoricalRangeOutcomeStatus]) -> HistoricalRangeOutcomeStatus:
    if HistoricalRangeOutcomeStatus.FAILED in statuses:
        return HistoricalRangeOutcomeStatus.FAILED
    if HistoricalRangeOutcomeStatus.MATURING in statuses:
        return HistoricalRangeOutcomeStatus.MATURING
    if HistoricalRangeOutcomeStatus.NOT_DUE in statuses:
        return HistoricalRangeOutcomeStatus.NOT_DUE
    if HistoricalRangeOutcomeStatus.CENSORED in statuses:
        return HistoricalRangeOutcomeStatus.CENSORED
    if HistoricalRangeOutcomeStatus.TERMINAL in statuses:
        return HistoricalRangeOutcomeStatus.TERMINAL
    return HistoricalRangeOutcomeStatus.COMPLETE
