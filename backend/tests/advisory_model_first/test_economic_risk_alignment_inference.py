from __future__ import annotations

from dataclasses import replace
from datetime import date

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_contracts import ECONOMIC_FEATURE_NAMES
from backend.services.advisory_model_first.economic_risk_alignment_contracts import (
    EconomicModelScopeV2, EntryRiskBudgetV2, PredictionInputContextV2,
)
from backend.services.advisory_model_first.economic_risk_alignment_inference import (
    build_daily_entry_loss_advice_v2, price_context_sha256, select_daily_entry_loss_advice_v2,
)
from backend.services.advisory_model_first.economic_risk_alignment_training import EntryLossTrainingResultV2
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.realtime_feature_source import PriceRangeRealtimeContext

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


class _Head:
    """Decision-contract fixture, NOT an effective or trained business model."""

    def __init__(self, risk=False):
        self.risk = risk

    def predict(self, frame, **kwargs):
        return np.full(len(frame), 100.) if self.risk else np.where(np.isclose(frame.query_gap_bps, 10.), -1., 1.)


def _daily(study):
    fitted = EntryLossTrainingResultV2(
        _Head(), _Head(True), study["request"], "a" * 64, "b" * 64,
        {name: (-1., 100.) for name in ECONOMIC_FEATURE_NAMES},
        {0: {"observation_count": 50, "decision_day_count": 10,
             "observed_min_gap_bps": 0., "observed_max_gap_bps": 50.}},
        {"uncertainty_semantics": "validation_error_not_mean_ci", "validation_return_abs_error_p90_bps": 50.},
        pd.DataFrame(),
    )
    identity = study["identity"]
    scope = EconomicModelScopeV2(package_id=identity.package_id, manifest_sha256=identity.manifest_sha256,
                                selection_runtime_semantics_hash=identity.selection_runtime_semantics_hash,
                                feature_schema_sha256="c" * 64, shadow_policy_sha256=identity.shadow_policy_sha256,
                                cost_policy_sha256=identity.cost_policy_sha256, coordinate_algorithm_sha256="d" * 64)
    decision, target = study["request"].test_start, date(2025, 1, 29)
    context = PriceRangeRealtimeContext(symbol="000001.SZ", decision_raw_close=10., decision_price_trade_date=decision,
                                       decision_price_source="fixture", price_unit_divisor=1000,
                                       target_raw_price_multiplier=1., corporate_action_source="fixture",
                                       board_type="MAIN", list_date=date(2020, 1, 1), listed_trading_days=1200,
                                       target_is_st=False, tick_size=.01)
    prediction = PredictionInputContextV2(
        scope=scope, decision_date=decision, target_date=target, feature_visible_through=decision,
        price_visible_through=decision, instrument=context.symbol, selection_rank=1, run_id="daily_run", list_id="daily_list",
        candidate_roster_sha256="e" * 64, feature_source_sha256="f" * 64, price_context_sha256=price_context_sha256(context),
        source_evidence="RECOVERED_LIMITED", evidence_limitations=("fixture not business evidence",),
    )
    budget = EntryRiskBudgetV2(maximum_loss_bps=800, reference_use="FIXED_RESEARCH_STOP_REFERENCE", configuration_sha256="1" * 64)
    return dict(fitted=fitted, model_scope=scope, model_bundle_sha256="2" * 64, prediction_input=prediction,
                context=context, decision_features={name: .5 for name in ECONOMIC_FEATURE_NAMES if name != "query_gap_bps"},
                trading_calendar=[decision, target], risk_budget=budget)


def test_new_daily_identity_does_not_require_old_training_hash_and_disjoint_prices(study):
    args = _daily(study)
    advice = build_daily_entry_loss_advice_v2(**args)
    assert advice["training_input_identity_sha256"] != advice["prediction_input_sha256"]
    assert len(advice["acceptable_price_intervals"]) == 2  # No bridge over rejected 10.01.
    assert not advice["deployable"] and not advice["production_scope_eligible"]
    for price, action in [(10., "TAKE"), (10.01, "SKIP"), (9., "UNAVAILABLE")]:
        chosen = select_daily_entry_loss_advice_v2(advice=advice, expected_advice_sha256=advice["advice_sha256"],
                                                 observation_date=args["prediction_input"].target_date,
                                                 actual_open_raw_cny=price, trading_status="EXECUTABLE")
        assert chosen["action"] == action
    changed = args["prediction_input"].model_copy(update={"candidate_roster_sha256": "3" * 64, "run_id": "new_run"})
    assert build_daily_entry_loss_advice_v2(**{**args, "prediction_input": changed})["prediction_input_sha256"] != advice["prediction_input_sha256"]


def test_unconfigured_budget_returns_estimates_not_false_skip_or_take(study):
    advice = build_daily_entry_loss_advice_v2(**{**_daily(study), "risk_budget": None})
    assert advice["recommendation_status"] == "RISK_CONTRACT_UNCONFIGURED"
    assert not advice["acceptable_price_intervals"]
    assert any(value["expected_net_return_bps"] is not None for value in advice["query_nodes"])
    args = _daily(study)
    args["decision_features"]["ret_1"] = None
    unknown = build_daily_entry_loss_advice_v2(**{**args, "risk_budget": None})
    assert unknown["recommendation_status"] == "UNAVAILABLE" and unknown["availability"] == "UNAVAILABLE"


@pytest.mark.parametrize("defect", ["scope", "price_hash", "calendar", "future_price", "bad_tick"])
def test_daily_identity_pit_or_regulatory_contradictions_fail_closed(study, defect):
    args = _daily(study)
    if defect == "scope":
        args["model_scope"] = args["model_scope"].model_copy(update={"cost_policy_sha256": "4" * 64})
    elif defect == "price_hash":
        args["context"] = replace(args["context"], decision_raw_close=20.)
    elif defect == "calendar":
        args["trading_calendar"] = [args["prediction_input"].decision_date, date(2025, 1, 28), args["prediction_input"].target_date]
    elif defect == "future_price":
        args["prediction_input"] = args["prediction_input"].model_copy(update={"price_visible_through": date(2025, 1, 29)})
    else:
        args["context"] = replace(args["context"], tick_size=-.01)
        args["prediction_input"] = args["prediction_input"].model_copy(update={"price_context_sha256": price_context_sha256(args["context"])})
    with pytest.raises((AdvisoryModelFirstError, ValueError)):
        build_daily_entry_loss_advice_v2(**args)


def test_grid_over_budget_and_unlimited_domain_are_explicit_not_truncated(study):
    args = _daily(study)
    limited = build_daily_entry_loss_advice_v2(**{**args, "maximum_grid_nodes": 2})
    assert limited["reason_code"] == "GRID_RESOURCE_BUDGET_EXCEEDED" and not limited["query_nodes"]
    args["context"] = replace(args["context"], board_type="STAR", list_date=args["prediction_input"].decision_date, listed_trading_days=2)
    args["prediction_input"] = args["prediction_input"].model_copy(update={"price_context_sha256": price_context_sha256(args["context"])})
    unlimited = build_daily_entry_loss_advice_v2(**args)
    assert unlimited["reason_code"] == "NO_DAILY_LIMIT_NEEDS_REGISTERED_DOMAIN"


def test_frozen_advice_hash_prevents_after_outcome_rewriting(study):
    args = _daily(study)
    advice = build_daily_entry_loss_advice_v2(**args)
    advice["query_nodes"][0]["status"] = "ACCEPTABLE"
    with pytest.raises(AdvisoryModelFirstError):
        select_daily_entry_loss_advice_v2(advice=advice, expected_advice_sha256=advice["advice_sha256"],
                                         observation_date=args["prediction_input"].target_date,
                                         actual_open_raw_cny=9., trading_status="EXECUTABLE")
