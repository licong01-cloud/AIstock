from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from backend.services.advisory_modeling.contracts import (
    DIAGNOSTIC_SEEDS,
    PRIMARY_SEED,
    ExperimentResultV1,
    FeatureSet,
    frozen_experiment_registry_v1,
    select_research_configuration,
)
from backend.services.advisory_modeling.market_regime import (
    FeatureFitStatisticsV1,
    FittedMarketRegimePolicyV1,
    MarketRegime,
    MarketRegimePolicyTemplateV1,
)
from backend.services.advisory_modeling.errors import (
    AdvisoryModelingError,
    REASON_SELECTION_NOT_UNIQUE,
)
from backend.services.advisory_modeling.training_view import (
    FoldEvidenceClosureV1,
    build_split_plan,
)


_HASH = "a" * 64


def _fold_evidence() -> tuple[FoldEvidenceClosureV1, ...]:
    return tuple(
        FoldEvidenceClosureV1(
            fold_index=index,
            available_observation_set_hash=f"{index + 1:x}" * 64,
            available_label_set_hash=f"{index + 6:x}" * 64,
            available_member_set_hash=f"{index + 11:x}" * 64,
        )
        for index in range(5)
    )


def _weekdays(start: date, count: int) -> tuple[date, ...]:
    values: list[date] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(current)
        current += timedelta(days=1)
    return tuple(values)


def test_split_plan_uses_five_non_overlapping_60_date_outer_blocks() -> None:
    calendar = _weekdays(date(2018, 1, 1), 2200)
    eligible = calendar[1300:1800]
    plan = build_split_plan(
        request_semantic_hash=_HASH,
        calendar_hash="b" * 64,
        trading_dates=calendar,
        eligible_decision_dates=eligible,
        fold_evidence_closures=_fold_evidence(),
    )

    assert plan.coverage_status == "COMPLETE"
    assert len(plan.folds) == 5
    assert tuple(day for fold in plan.folds for day in fold.test_dates) == eligible[-300:]
    for fold in plan.folds:
        assert len(fold.pre_test_embargo_dates) == 20
        assert len(fold.validation_dates) == 60
        assert len(fold.fit_validation_gap_dates) == 20
        assert fold.training_windows[0].fit_dates[-1] < fold.fit_validation_gap_dates[0]
        assert fold.fold_training_as_of.date() == fold.pre_test_embargo_dates[-1]
        assert fold.fold_training_as_of.hour == 7
        assert fold.fold_training_as_of.minute == 0
        assert fold.label_availability_as_of == fold.fold_training_as_of
        assert fold.test_end_calendar_position - fold.test_start_calendar_position + 1 == 60


def test_split_plan_reports_coverage_instead_of_partial_folds() -> None:
    calendar = _weekdays(date(2020, 1, 1), 500)
    plan = build_split_plan(
        request_semantic_hash=_HASH,
        calendar_hash="b" * 64,
        trading_dates=calendar,
        eligible_decision_dates=calendar[-299:],
    )
    assert plan.coverage_status == "INSUFFICIENT_ELIGIBLE_DATES"
    assert plan.folds == ()

    early_calendar = _weekdays(date(2020, 1, 1), 400)
    early = build_split_plan(
        request_semantic_hash=_HASH,
        calendar_hash="b" * 64,
        trading_dates=early_calendar,
        eligible_decision_dates=early_calendar[:300],
    )
    assert early.coverage_status == "INSUFFICIENT_SPLIT_HISTORY"


def test_complete_split_rejects_missing_fold_evidence_closure() -> None:
    calendar = _weekdays(date(2018, 1, 1), 2200)
    with pytest.raises(ValueError, match="evidence closures"):
        build_split_plan(
            request_semantic_hash=_HASH,
            calendar_hash="b" * 64,
            trading_dates=calendar,
            eligible_decision_dates=calendar[1300:1800],
        )


def test_split_plan_accepts_sparse_eligible_test_dates_and_preserves_positions() -> None:
    calendar = _weekdays(date(2015, 1, 1), 3000)
    eligible = calendar[1800:2600:2]
    plan = build_split_plan(
        request_semantic_hash=_HASH,
        calendar_hash="b" * 64,
        trading_dates=calendar,
        eligible_decision_dates=eligible,
        fold_evidence_closures=_fold_evidence(),
    )

    assert plan.coverage_status == "COMPLETE"
    assert len(plan.folds[0].test_calendar_positions) == 60
    assert plan.folds[0].test_end_calendar_position - plan.folds[0].test_start_calendar_position > 59


def test_split_plan_marks_truncated_multi_year_window_instead_of_claiming_complete() -> None:
    calendar = _weekdays(date(2022, 1, 1), 1500)
    eligible = calendar[700:1300]
    plan = build_split_plan(
        request_semantic_hash=_HASH,
        calendar_hash="b" * 64,
        trading_dates=calendar,
        eligible_decision_dates=eligible,
        fold_evidence_closures=_fold_evidence(),
    )

    assert plan.coverage_status == "INSUFFICIENT_CALENDAR_HISTORY"
    assert any(
        window.coverage_status == "INSUFFICIENT_CALENDAR_HISTORY"
        for fold in plan.folds
        for window in fold.training_windows
    )


def _result(
    feature_set: FeatureSet,
    *,
    years: int = 2,
    ndcg: str = "0.02",
    return_uplift: str = "0.001",
    mae: str = "1.0",
    turnover: str = "0.1",
) -> ExperimentResultV1:
    return ExperimentResultV1(
        candidate_experiment_id=f"short-rebound-{years}y-{feature_set.value.lower()}",
        training_window_years=years,
        feature_set=feature_set,
        completed_fold_count=5,
        modelable_coverage=Decimal("0.95"),
        contract_error_count=0,
        completed_seed_set=(PRIMARY_SEED, *DIAGNOSTIC_SEEDS),
        ndcg_at_5_uplift_lower_bound_95=Decimal(ndcg),
        mean_net_excess_return_5_uplift=Decimal(return_uplift),
        executable_mae_loss_ratio=Decimal(mae),
        turnover_uplift=Decimal(turnover),
        primary_fold_best_iterations=(100, 200, 300, 400, 500),
    )


def _all_results() -> tuple[ExperimentResultV1, ...]:
    return tuple(
        _result(definition.feature_set, years=definition.training_window_years)
        for definition in frozen_experiment_registry_v1().candidates
    )


def test_experiment_registry_is_exactly_twelve_preregistered_candidates() -> None:
    registry = frozen_experiment_registry_v1()
    assert len(registry.candidates) == 12
    assert {item.training_window_years for item in registry.candidates} == {2, 3, 5}
    assert {item.feature_set for item in registry.candidates} == set(FeatureSet)
    assert registry.training_config.num_threads == 1
    assert registry.training_config.deterministic is True


def test_model_selection_uses_quantized_immutable_lexicographic_order() -> None:
    registry = frozen_experiment_registry_v1()
    core = _result(FeatureSet.CORE, ndcg="0.0200000000004")
    hmm = _result(FeatureSet.CORE_PLUS_HMM, ndcg="0.0200000000003")
    replacements = {
        core.candidate_experiment_id: core,
        hmm.candidate_experiment_id: hmm,
    }
    results = tuple(
        replacements.get(item.candidate_experiment_id, item) for item in _all_results()
    )

    receipt = select_research_configuration(registry=registry, results=results)

    assert receipt.selected_experiment_id == core.candidate_experiment_id
    assert receipt.final_n_estimators == 300


def test_model_selection_rejects_unregistered_or_ineligible_only_results() -> None:
    registry = frozen_experiment_registry_v1()
    ineligible_results = []
    for result in _all_results():
        payload = result.model_dump(mode="python")
        payload["completed_fold_count"] = 4
        payload["primary_fold_best_iterations"] = payload["primary_fold_best_iterations"][:4]
        payload["reason_codes"] = ("MODEL_OOS_SAMPLE_INSUFFICIENT",)
        payload["result_hash"] = None
        ineligible_results.append(ExperimentResultV1.model_validate(payload))
    with pytest.raises(AdvisoryModelingError, match="no research-eligible") as error:
        select_research_configuration(registry=registry, results=tuple(ineligible_results))
    assert error.value.reason_code == REASON_SELECTION_NOT_UNIQUE


def test_market_regime_uses_fold_statistics_and_missing_is_unavailable() -> None:
    template = MarketRegimePolicyTemplateV1()
    policy = FittedMarketRegimePolicyV1(
        policy_template_hash=str(template.policy_template_hash),
        fold_id="fold-0",
        universe_policy_set_hash=_HASH,
        calendar_hash="b" * 64,
        decision_cutoff_hash="c" * 64,
        return_statistics=FeatureFitStatisticsV1(
            feature_id=template.return_feature_id,
            mean=Decimal("0"),
            sample_std=Decimal("0.1"),
            sample_count=200,
        ),
        breadth_statistics=FeatureFitStatisticsV1(
            feature_id=template.breadth_feature_id,
            mean=Decimal("0.5"),
            sample_std=Decimal("0.1"),
            sample_count=200,
        ),
    )

    assert policy.classify(
        pit_universe_equal_weight_return_20=Decimal("0.1"),
        market_breadth_above_ma20=Decimal("0.6"),
    ) is MarketRegime.BULL
    assert policy.classify(
        pit_universe_equal_weight_return_20=None,
        market_breadth_above_ma20=Decimal("0.5"),
    ) is MarketRegime.UNAVAILABLE

    zero_variance_payload = policy.model_dump(mode="python")
    zero_variance_payload["return_statistics"]["sample_std"] = Decimal("0")
    zero_variance_payload["return_statistics"]["statistics_hash"] = None
    zero_variance_payload["fitted_market_regime_policy_hash"] = None
    zero_variance = FittedMarketRegimePolicyV1.model_validate(zero_variance_payload)
    assert zero_variance.classify(
        pit_universe_equal_weight_return_20=Decimal("0"),
        market_breadth_above_ma20=Decimal("0.5"),
    ) is MarketRegime.UNAVAILABLE
