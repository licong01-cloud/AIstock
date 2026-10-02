from __future__ import annotations

from dataclasses import replace
from datetime import date

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_contracts import (
    ECONOMIC_FEATURE_NAMES,
    EconomicEntryInputIdentityV1,
    EconomicEntryLabelV1,
    EconomicEntryTrainingRequestV1,
)
from backend.services.advisory_model_first.economic_entry_inference import (
    build_economic_price_advice,
    select_frozen_opening_advice,
)
from backend.services.advisory_model_first.economic_entry_training import (
    EconomicEntryTrainingResult,
    prepare_economic_training_rows,
    train_economic_entry_model,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicyCostV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


@pytest.fixture
def study():
    days = pd.bdate_range("2025-01-02", periods=24)
    policy = {
        "target_count": 5, "rank_enter_threshold": 5, "rank_exit_threshold": 40,
        "rank_exit_confirm_days": 2, "daily_replacement_budget": 5,
        "stop_loss_bps": 800, "take_profit_bps": 1800, "trailing_stop_bps": 700,
        "time_stop_days": 20, "take_profit_mode": "trailing",
        "entry_price_basis": "next_open_executable", "exit_price_basis": "next_open_executable",
    }
    cost = AdvisoryPolicyCostV1(buy_cost_bps=.95, sell_cost_bps=5.95)
    identity = EconomicEntryInputIdentityV1(
        dataset_manifest_sha256="1" * 64, request_id="request", request_sha256="2" * 64,
        program_id="program", binding_version_id="binding", package_id="package", manifest_sha256="3" * 64,
        selection_runtime_semantics_hash="4" * 64, universe_identity_sha256="5" * 64,
        candidate_roster_sha256="6" * 64, price_coordinate_sha256="7" * 64,
        price_source_sha256="8" * 64, reference_source_sha256="9" * 64, shadow_policy=policy,
        shadow_policy_sha256=canonical_json_sha256(policy), cost_policy=cost, cost_policy_sha256=cost.policy_sha256,
        source_evidence="RECOVERED_LIMITED", evidence_limitations=("synthetic unit fixture, not business evidence",),
    )
    request = EconomicEntryTrainingRequestV1(
        input_identity_sha256=identity.identity_sha256, feature_source_sha256="a" * 64,
        implementation_sha256="b" * 64, train_start=days[0].date(), train_end=days[10].date(),
        validation_start=days[11].date(), validation_end=days[17].date(), test_start=days[18].date(),
        test_end=days[20].date(), label_cutoff=days[-1].date(), downside_budget_bps=800,
    )
    labels, features = [], []
    for index, day in enumerate(days[:21]):
        for symbol_index in range(6):
            value = symbol_index * 5 - 10.
            labels.append(EconomicEntryLabelV1(
                input_identity_sha256=identity.identity_sha256, episode_label_id=f"episode_{index}_{symbol_index}",
                decision_date=day.date(), target_date=days[index + 1].date(),
                instrument=f"{symbol_index + 1:06d}.SZ", selection_rank=symbol_index + 1,
                original_label_status="MATURED", label_information_end=days[index + 2].date(), status="AVAILABLE",
                decision_raw_close_cny=10, target_reference_raw_cny=10,
                actual_open_raw_cny=10 * (1 + symbol_index * 10 / 10000),
                actual_gap_bps=float(symbol_index * 10), enter_net_value_bps=value, skip_net_value_bps=0,
                entry_advantage_bps=value, daily_mark_max_drawdown_bps=100 + symbol_index * 10,
            ))
            features.append({
                "decision_as_of_trade_date": day, "target_trade_date": days[index + 1],
                "instrument": f"{symbol_index + 1:06d}.SZ", "feature_visible_through": day,
                "feature_source_sha256": request.feature_source_sha256,
                **{name: .5 for name in ECONOMIC_FEATURE_NAMES if name != "query_gap_bps"},
            })
    return dict(identity=identity, request=request, labels=tuple(labels), features=pd.DataFrame(features))


def test_purge_and_future_test_poison_do_not_change_real_fixed_model(study):
    args = {key: study[key] for key in ("features", "labels", "request")}
    rows = prepare_economic_training_rows(**args)
    purged = rows[rows["split"].eq("PURGED_LABEL_END")]
    assert len(purged) == 24  # Last two D groups in train and validation have unfinished labels.
    first = train_economic_entry_model(**args)
    poisoned = tuple(label.model_copy(update={"entry_advantage_bps": 9999, "enter_net_value_bps": 9999})
                     if label.decision_date >= study["request"].test_start else label for label in study["labels"])
    study["features"]["target_high"] = 1e9
    study["features"]["query_gap_bps"] = -9999  # Caller-supplied future condition must be ignored.
    second = train_economic_entry_model(features=study["features"], labels=poisoned, request=study["request"])
    assert first.return_model.model_to_string() == second.return_model.model_to_string()
    assert first.risk_model.model_to_string() == second.risk_model.model_to_string()
    assert first.diagnostics == second.diagnostics
    assert first.price_support[0]["observation_count"] == 54
    assert first.diagnostics["test_used_for_training_or_calibration"] is False


@pytest.mark.parametrize("defect", ["future_feature", "label_identity", "missing_roster", "duplicate_feature"])
def test_training_input_contradictions_fail_closed(study, defect):
    if defect == "future_feature":
        study["features"].loc[0, "feature_visible_through"] = pd.Timestamp(study["request"].test_start)
    elif defect == "label_identity":
        study["request"] = study["request"].model_copy(update={"input_identity_sha256": "c" * 64})
    elif defect == "missing_roster":
        study["features"] = study["features"].iloc[1:]
    else:
        study["features"] = pd.concat([study["features"], study["features"].iloc[[0]]])
    with pytest.raises(AdvisoryModelFirstError):
        prepare_economic_training_rows(**{key: study[key] for key in ("features", "labels", "request")})


class _GridHead:
    """Inference-only test double, not training or economic-effectiveness evidence."""

    def __init__(self, mode):
        self.mode = mode

    def predict(self, frame, **kwargs):
        if self.mode == "risk":
            return np.full(len(frame), 100.)
        if self.mode == "zero":
            return np.zeros(len(frame))
        return np.where(np.isclose(frame["query_gap_bps"], 10), -5., 5.)


def _advice(study, *, mode="return", price_support=None):
    fitted = EconomicEntryTrainingResult(
        _GridHead(mode), _GridHead("risk"), study["request"],
        {name: (-1., 100.) for name in ECONOMIC_FEATURE_NAMES},
        {0: {"observation_count": 50, "decision_day_count": 10,
             "observed_min_gap_bps": 0., "observed_max_gap_bps": 50.}} if price_support is None else price_support,
        {"uncertainty_semantics": "validation_error_not_mean_ci", "validation_return_abs_error_p90_bps": 50.},
        pd.DataFrame(),
    )
    args = dict(
        fitted=fitted, model_bundle_sha256="c" * 64, input_identity=study["identity"], instrument="000001.SZ",
        decision_date=study["request"].test_start, target_date=date(2025, 1, 29),
        feature_visible_through=study["request"].test_start,
        decision_features={name: .5 for name in ECONOMIC_FEATURE_NAMES if name != "query_gap_bps"},
        reference_raw_cny=10., legal_price_min_cny=9., legal_price_max_cny=11.,
        query_prices_cny=[10., 10.01, 10.02], grid_step_cny=.01, tick_size_cny=.01,
    )
    return build_economic_price_advice(**args), args


def test_non_contiguous_values_zero_value_and_unknown_support_are_distinct(study):
    advice, args = _advice(study)
    assert [(row["minimum_cny"], row["maximum_cny"]) for row in advice["acceptable_price_intervals"]] == [(10., 10.), (10.02, 10.02)]
    assert advice["source_evidence"] == "RECOVERED_LIMITED"
    assert _advice(study, mode="zero")[0]["recommendation_status"] == "NO_ACCEPTABLE_PRICE"
    assert _advice(study, price_support={})[0]["recommendation_status"] == "UNAVAILABLE"
    # Future path extras cannot alter the D-frozen value surface.
    args["decision_features"]["target_low"] = .001
    assert build_economic_price_advice(**args)["advice_sha256"] == advice["advice_sha256"]
    args["fitted"] = replace(args["fitted"], price_support={})
    assert build_economic_price_advice(**args)["acceptable_price_intervals"] == []


def test_opening_selection_is_exact_frozen_nodes_and_never_recommends_unknown(study):
    advice, args = _advice(study)
    selection = dict(advice=advice, expected_advice_sha256=advice["advice_sha256"],
                     observation_date=args["target_date"], trading_status="EXECUTABLE")
    assert select_frozen_opening_advice(**selection, actual_open_raw_cny=10.)["action"] == "TAKE"
    assert select_frozen_opening_advice(**selection, actual_open_raw_cny=10.01)["action"] == "SKIP"
    assert select_frozen_opening_advice(**selection, actual_open_raw_cny=9.2)["action"] == "UNAVAILABLE"
    assert select_frozen_opening_advice(**selection, actual_open_raw_cny=None)["action"] == "UNAVAILABLE"
    selection["trading_status"] = "SUSPENDED"
    assert select_frozen_opening_advice(**selection, actual_open_raw_cny=None)["action"] == "NOT_APPLICABLE"
    advice["query_nodes"][1]["status"] = "ACCEPTABLE"
    with pytest.raises(AdvisoryModelFirstError, match="D-frozen advice identity"):
        select_frozen_opening_advice(**selection, actual_open_raw_cny=10.01)


@pytest.mark.parametrize("defect", ["future_clock", "omitted_grid_node", "off_tick", "policy_identity", "support_count"])
def test_inference_clock_price_and_support_contracts_fail_closed(study, defect):
    _, args = _advice(study)
    if defect == "future_clock":
        args["feature_visible_through"] = args["target_date"]
    elif defect == "omitted_grid_node":
        args["query_prices_cny"] = [10., 10.02]
    elif defect == "off_tick":
        args["query_prices_cny"] = [10.005, 10.015]
    elif defect == "policy_identity":
        args["fitted"] = replace(args["fitted"], request=args["fitted"].request.model_copy(update={"downside_budget_bps": 900}))
    else:
        args["fitted"].price_support[0]["observation_count"] = 1
    with pytest.raises(AdvisoryModelFirstError):
        build_economic_price_advice(**args)


def test_missing_required_value_is_unavailable_not_zero_return_or_skip(study):
    _, args = _advice(study)
    args["decision_features"]["ret_1"] = None
    advice = build_economic_price_advice(**args)
    assert advice["recommendation_status"] == "UNAVAILABLE"
    assert all(node["expected_net_return_bps"] is None for node in advice["query_nodes"])


def test_partial_unknown_grid_cannot_be_reported_as_all_prices_rejected(study):
    _, args = _advice(study, mode="zero")
    args["fitted"].price_support[0]["observed_max_gap_bps"] = 15
    advice = build_economic_price_advice(**args)
    assert advice["availability"] == "PARTIAL"
    assert advice["recommendation_status"] == "UNAVAILABLE"
    assert advice["query_nodes"][0]["status"] == "REJECTED"
    assert advice["query_nodes"][-1]["status"] == "OUT_OF_SUPPORT"
