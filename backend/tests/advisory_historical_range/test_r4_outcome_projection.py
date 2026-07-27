from __future__ import annotations

from datetime import date
from decimal import Decimal
import inspect

import pytest

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeEvaluationWindowType,
    HistoricalRangeOutcomeArtifactV2,
    HistoricalRangeOutcomeProjection,
    HistoricalRangeOutcomeRevisionReason,
    HistoricalRangeOutcomeStatus,
    HistoricalRangeOutcomeSubjectType,
    HistoricalRangeOutcomeWorkItemV1,
)
from backend.services.advisory_historical_range.outcome_projection import (
    EpisodeLifecycleOutcomeEngine,
    ExecutablePathOutcomeEngine,
    HistoricalRangeProjectionError,
    HistoricalRangeOutcomeProjectionBuilder,
    RecommendationPathOutcomeEngine,
)
from backend.services.advisory_historical_range.retrospective_projection import (
    PostgresHistoricalRangeCandidateProjectionLoader,
)
from backend.services.advisory_phase1.label_policy import Projection
from backend.services.advisory_phase1.outcome_engine import (
    DailyPriceBar,
    OutcomeEngine,
    PositionPathValuationCore,
    TerminalDisposition,
    TerminalResolution,
)
from backend.tests.advisory_phase1.test_outcome_engine import AS_OF, _request, _source_binding


HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64


def test_candidate_projection_uses_the_durable_successful_day_status() -> None:
    source = inspect.getsource(PostgresHistoricalRangeCandidateProjectionLoader._load_row)

    assert "day.status = 'COMPLETE'" in source
    assert "day.status = 'COMPLETED'" not in source


def _ref(kind: HistoricalRangeArtifactKind, digest: str) -> HistoricalRangeArtifactRefV1:
    namespace = {
        HistoricalRangeArtifactKind.REQUEST: "requests",
        HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT: "candidate-artifacts",
        HistoricalRangeArtifactKind.OUTCOME: "outcomes",
    }[kind]
    return HistoricalRangeArtifactRefV1(
        artifact_kind=kind,
        relative_path=f"{namespace}/{digest}.json",
        producer_contract_version="test_v1",
        payload_schema_version="test_v1",
        semantic_content_hash=digest,
        payload_sha256=digest,
        file_sha256=digest,
    )


def test_formal_outcome_engine_delegates_with_field_and_hash_parity() -> None:
    request = _request(Projection.RETURN_NET_EXCESS)
    expected = PositionPathValuationCore().calculate(request)
    actual = OutcomeEngine().calculate(request)
    assert actual.model_dump(mode="json") == expected.model_dump(mode="json")
    assert actual.projection_payload_hash == expected.projection_payload_hash
    assert actual.calculation_evidence.evidence_hash == expected.calculation_evidence.evidence_hash


def test_recommendation_and_executable_entry_semantics_remain_separate() -> None:
    recommendation_request = _request(Projection.RETURN_GROSS, path=None)
    calendar = recommendation_request.policies.calendar
    decision = recommendation_request.owner.decision_as_of_trade_date
    timeline = calendar.timeline(decision_date=decision, horizon_trading_days=1)
    path = recommendation_request.price_path.model_copy(
        update={
            "bars": tuple(
                DailyPriceBar.model_validate(
                    {
                        **bar.model_dump(mode="python", exclude={"source_hash"}),
                        "entry_executable": False,
                    }
                )
                if bar.trade_date == timeline[1]
                else bar
                for bar in recommendation_request.price_path.bars
            )
        }
    )
    recommendation_request = recommendation_request.model_copy(update={"price_path": path})
    recommendation = RecommendationPathOutcomeEngine().calculate(
        requests={Projection.RETURN_GROSS: recommendation_request},
        timeline=timeline,
        evaluation_window_type=HistoricalRangeEvaluationWindowType.FIXED_HORIZON,
        horizon_trade_days=1,
    )
    executable = ExecutablePathOutcomeEngine().calculate(
        requests={Projection.RETURN_GROSS: recommendation_request},
        timeline=timeline,
        evaluation_window_type=HistoricalRangeEvaluationWindowType.FIXED_HORIZON,
        horizon_trade_days=1,
    )
    assert recommendation.calculation_results[0].projection_value_decimal is not None
    assert recommendation.calculation_results[0].entry_price_raw_yuan == Decimal("10")
    assert executable.calculation_results[0].projection_value_decimal is None
    assert executable.calculation_results[0].entry_status.value == "NOT_EXECUTABLE"


def test_work_item_identity_closes_window_policy_source_and_revision() -> None:
    subject_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, HASH_A)
    source_ref = _ref(HistoricalRangeArtifactKind.OUTCOME, HASH_B)
    item = HistoricalRangeOutcomeWorkItemV1(
        range_run_id="run-1",
        subject_type=HistoricalRangeOutcomeSubjectType.CANDIDATE,
        subject_id="candidate-1",
        subject_ref=subject_ref,
        policy_bundle_ref=_ref(HistoricalRangeArtifactKind.REQUEST, HASH_C),
        projection=HistoricalRangeOutcomeProjection.EXECUTABLE,
        evaluation_window_type=HistoricalRangeEvaluationWindowType.FIXED_HORIZON,
        horizon_trade_days=5,
        policy_bundle_hash=HASH_C,
        decision_trade_date=date(2026, 7, 1),
        intended_entry_trade_date=date(2026, 7, 2),
        earliest_sell_trade_date=date(2026, 7, 3),
        exit_trade_date=date(2026, 7, 9),
        label_as_of_trade_date=date(2026, 7, 10),
        source_revision_refs=(source_ref,),
        source_revision_set_hash=canonical_json_sha256([source_ref.model_dump(mode="json")]),
        producer_code_hash=HASH_A,
        outcome_contract_version="r4_v1",
        revision_reason=HistoricalRangeOutcomeRevisionReason.INITIAL,
    )
    changed = item.model_copy(update={"label_as_of_trade_date": date(2026, 7, 11), "outcome_input_hash": None})
    changed = HistoricalRangeOutcomeWorkItemV1.model_validate(changed.model_dump(mode="python"))
    assert changed.outcome_logical_id == item.outcome_logical_id
    assert changed.outcome_input_hash != item.outcome_input_hash


def test_episode_window_requires_zero_sentinel_and_episode_subject() -> None:
    subject_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, HASH_A)
    source_ref = _ref(HistoricalRangeArtifactKind.OUTCOME, HASH_B)
    source_hash = canonical_json_sha256([source_ref.model_dump(mode="json")])
    with pytest.raises(ValueError, match="episode lifecycle"):
        HistoricalRangeOutcomeWorkItemV1(
            range_run_id="run-1",
            subject_type=HistoricalRangeOutcomeSubjectType.CANDIDATE,
            subject_id="candidate-1",
            subject_ref=subject_ref,
            policy_bundle_ref=_ref(HistoricalRangeArtifactKind.REQUEST, HASH_C),
            projection=HistoricalRangeOutcomeProjection.RECOMMENDATION,
            evaluation_window_type=HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE,
            horizon_trade_days=0,
            policy_bundle_hash=HASH_C,
            decision_trade_date=date(2026, 7, 1),
            label_as_of_trade_date=date(2026, 7, 10),
            source_revision_refs=(source_ref,),
            source_revision_set_hash=source_hash,
            producer_code_hash=HASH_A,
            outcome_contract_version="r4_v1",
            revision_reason=HistoricalRangeOutcomeRevisionReason.INITIAL,
        )


def test_correction_artifact_requires_predecessor_and_revision_evidence_upstreams() -> None:
    subject_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, HASH_A)
    policy_ref = _ref(HistoricalRangeArtifactKind.REQUEST, HASH_C)
    predecessor_ref = _ref(HistoricalRangeArtifactKind.OUTCOME, HASH_B)
    with pytest.raises(ValueError, match="predecessor must be an exact upstream"):
        HistoricalRangeOutcomeArtifactV2(
            outcome_logical_id="logical-1",
            outcome_version_id="version-2",
            outcome_input_hash=HASH_C,
            subject_ref=subject_ref,
            direct_upstream_refs=(subject_ref, policy_ref),
            projection_group=HistoricalRangeOutcomeProjection.EXECUTABLE,
            evaluation_window_type=HistoricalRangeEvaluationWindowType.FIXED_HORIZON,
            horizon_trade_days=1,
            policy_bundle_ref=policy_ref,
            policy_bundle_hash=HASH_C,
            label_as_of_trade_date=date(2026, 7, 10),
            source_revision_set_hash=HASH_A,
            maturity_status=HistoricalRangeOutcomeStatus.FAILED,
            reason_codes=("TEST_FAILURE",),
            calculation_result_set_hash=canonical_json_sha256([]),
            predecessor_outcome_ref=predecessor_ref,
            producer_code_hash=HASH_B,
        )

    builder_source = inspect.getsource(HistoricalRangeOutcomeProjectionBuilder.build_artifact)
    assert "work_item.predecessor_outcome_ref" in builder_source
    assert "work_item.revision_evidence_ref" in builder_source


def test_outcome_artifact_requires_exact_policy_ref_hash_upstream() -> None:
    subject_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, HASH_A)
    policy_ref = _ref(HistoricalRangeArtifactKind.REQUEST, HASH_C)

    with pytest.raises(ValueError, match="exact frozen policy bundle upstream"):
        HistoricalRangeOutcomeArtifactV2(
            outcome_logical_id="logical-1",
            outcome_version_id="version-1",
            outcome_input_hash=HASH_C,
            subject_ref=subject_ref,
            direct_upstream_refs=(subject_ref, policy_ref),
            projection_group=HistoricalRangeOutcomeProjection.EXECUTABLE,
            evaluation_window_type=HistoricalRangeEvaluationWindowType.FIXED_HORIZON,
            horizon_trade_days=1,
            policy_bundle_ref=policy_ref,
            policy_bundle_hash=HASH_B,
            label_as_of_trade_date=date(2026, 7, 10),
            source_revision_set_hash=HASH_A,
            maturity_status=HistoricalRangeOutcomeStatus.FAILED,
            reason_codes=("TEST_FAILURE",),
            calculation_result_set_hash=canonical_json_sha256([]),
            producer_code_hash=HASH_B,
        )


def test_open_episode_requires_exact_right_censor_and_emits_zero_sentinel() -> None:
    terminal = TerminalResolution(
        disposition=TerminalDisposition.RIGHT_CENSORED,
        symbol="000001.SZ",
        event_trade_date=date(2026, 7, 8),
        event_closed_at=AS_OF,
        source=_source_binding(),
        censor_reason_code="RANGE_END_ACTIVE",
    )
    request = _request(Projection.RETURN_GROSS, terminal=terminal, horizon=2)
    timeline = request.policies.calendar.timeline(
        decision_date=request.owner.decision_as_of_trade_date,
        horizon_trading_days=2,
    )

    result = EpisodeLifecycleOutcomeEngine().calculate(
        projection_group=HistoricalRangeOutcomeProjection.EXECUTABLE,
        requests={Projection.RETURN_GROSS: request},
        timeline=timeline,
        episode_closed=False,
    )

    assert result.horizon_trade_days == 0
    assert result.maturity_status.value == "CENSORED"
    assert result.calculation_results[0].horizon_trading_days == 0


def test_closed_episode_rejects_right_censor_evidence() -> None:
    terminal = TerminalResolution(
        disposition=TerminalDisposition.RIGHT_CENSORED,
        symbol="000001.SZ",
        event_trade_date=date(2026, 7, 8),
        event_closed_at=AS_OF,
        source=_source_binding(),
        censor_reason_code="RANGE_END_ACTIVE",
    )
    request = _request(Projection.RETURN_GROSS, terminal=terminal, horizon=2)
    timeline = request.policies.calendar.timeline(
        decision_date=request.owner.decision_as_of_trade_date,
        horizon_trading_days=2,
    )

    with pytest.raises(HistoricalRangeProjectionError, match="closed episode"):
        EpisodeLifecycleOutcomeEngine().calculate(
            projection_group=HistoricalRangeOutcomeProjection.EXECUTABLE,
            requests={Projection.RETURN_GROSS: request},
            timeline=timeline,
            episode_closed=True,
        )


def test_closed_matured_episode_calculations_include_observed_holding_days() -> None:
    request = _request(Projection.RETURN_GROSS, horizon=2)
    timeline = request.policies.calendar.timeline(
        decision_date=request.owner.decision_as_of_trade_date,
        horizon_trading_days=2,
    )

    result = EpisodeLifecycleOutcomeEngine().calculate(
        projection_group=HistoricalRangeOutcomeProjection.RECOMMENDATION,
        requests={Projection.RETURN_GROSS: request},
        timeline=timeline,
        episode_closed=True,
    )

    calculation = result.calculation_results[0]
    assert calculation.maturity_status.value == "MATURED"
    assert calculation.observed_holding_trading_days == 2
