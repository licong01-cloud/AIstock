from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_aligned_training import AlignedEntryTrainingResultV3
from backend.services.advisory_model_first.economic_entry_daily_contracts import EconomicEntryDailyInputV1
from backend.services.advisory_model_first.economic_entry_daily_inference import (
    build_economic_daily_price_set_v1, daily_decision_feature_values_v1, project_economic_daily_advice_v1,
    select_economic_daily_observed_price_v1,
)
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EconomicModelScopeV2, EntryRiskBudgetV2
from backend.services.advisory_model_first.economic_risk_alignment_inference import price_context_sha256
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.realtime_feature_source import PriceRangeRealtimeContext
from backend.services.strategy_package.runtime_variant import canonical_json_sha256
from backend.tests.advisory_model_first.test_economic_entry_aligned_training import _inputs
from backend.tests.advisory_model_first.test_economic_entry_model import _GridHead

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def _daily(study):
    # Inference-only doubles: no model fit or claimed economic/production proof.
    request = _inputs(study)["request"]
    names = request.source_request.feature_names
    fitted = AlignedEntryTrainingResultV3(_GridHead("return"), _GridHead("risk"), request,
        {name: (-1., 100.) for name in names},
        {0: {"observation_count": 50, "decision_day_count": 10, "observed_min_gap_bps": 0., "observed_max_gap_bps": 51.}},
        {"uncertainty_semantics": "validation_error_not_mean_ci", "validation_return_abs_error_p90_bps": 50.}, pd.DataFrame())
    identity = study["identity"]
    scope = EconomicModelScopeV2(package_id=identity.package_id, manifest_sha256=identity.manifest_sha256,
        selection_runtime_semantics_hash=identity.selection_runtime_semantics_hash, feature_schema_sha256="d" * 64,
        shadow_policy_sha256=identity.shadow_policy_sha256, cost_policy_sha256=identity.cost_policy_sha256,
        coordinate_algorithm_sha256="e" * 64)
    features = {name: .5 for name in names if name != "query_gap_bps"}
    context = PriceRangeRealtimeContext("000001.SZ", 10., request.source_request.test_start, "unit-only", 1000., 1.,
                                       "unit-only", "MAIN", date(2010, 1, 1), 99, False, .01)
    source = EconomicEntryDailyInputV1(scope=scope, program_id="program", binding_version_id="binding", run_id="run", list_id="list",
        decision_date=request.source_request.test_start, target_date=date(2025, 1, 29),
        feature_visible_through=request.source_request.test_start, price_visible_through=request.source_request.test_start,
        captured_at=datetime.now(timezone.utc), instrument=context.symbol, selection_rank=1, candidate_roster_sha256="6" * 64,
        feature_source_sha256="a" * 64, feature_values_sha256=canonical_json_sha256(daily_decision_feature_values_v1(features, names)),
        price_context_sha256=price_context_sha256(context), source_evidence="RECOVERED_LIMITED", evidence_level="HISTORICAL_REPLAY",
        evidence_limitations=("synthetic unit-only input, not business evidence",))
    return dict(fitted=fitted, model_scope=scope, model_bundle_sha256="c" * 64, prediction_input=source, context=context,
        decision_features=features, trading_calendar=[source.decision_date, source.target_date],
        risk_budget=EntryRiskBudgetV2(maximum_loss_bps=800, reference_use="FIXED_RESEARCH_STOP_REFERENCE", configuration_sha256="f" * 64))


def test_daily_grid_keeps_rejected_hole_unknown_and_compact_identity(study):
    args = _daily(study)
    advice = build_economic_daily_price_set_v1(**args)
    assert advice["recommendation_status"] == "ACCEPTABLE_PRICE_SET" and advice["availability"] == "PARTIAL"
    assert len(advice["acceptable_price_intervals"]) == 2
    assert advice["acceptable_price_intervals"][0]["maximum_cny"] == 10.
    assert advice["acceptable_price_intervals"][1]["minimum_cny"] == 10.02
    projected = project_economic_daily_advice_v1(advice)
    assert "query_nodes" not in projected and projected["original_advice_sha256"] == advice["advice_sha256"]
    args["decision_features"]["target_high"] = 1e12
    args["decision_features"]["query_gap_bps"] = -9999.
    assert build_economic_daily_price_set_v1(**args) == advice
    corrupted = {**advice, "recommendation_status": "NO_ACCEPTABLE_PRICE"}
    with pytest.raises(AdvisoryModelFirstError, match="immutable digest"):
        project_economic_daily_advice_v1(corrupted)
    # Even recomputing a file digest cannot turn contradictory contents into a
    # valid public projection (status, original ticks or interval membership).
    for changes, reason in (({"recommendation_status": "NO_ACCEPTABLE_PRICE"}, "recommendation contradicts"),
            ({"query_nodes": advice["query_nodes"][1:]}, "frozen price tick"),
            ({"acceptable_price_intervals": []}, "complete frozen nodes"),
            ({"node_status_counts": {**advice["node_status_counts"], "REJECTED": True}}, "complete frozen nodes")):
        payload = {key: value for key, value in {**advice, **changes}.items() if key != "advice_sha256"}
        with pytest.raises(AdvisoryModelFirstError, match=reason):
            project_economic_daily_advice_v1({**payload, "advice_sha256": canonical_json_sha256(payload)})


def test_daily_budget_and_normal_stale_price_do_not_fabricate_rejection(study, monkeypatch):
    args = _daily(study)
    import backend.services.advisory_model_first.economic_entry_daily_inference as module
    monkeypatch.setattr(module, "predict_aligned_entry_nodes_v3", lambda **kw: pytest.fail("preflight cannot allocate or predict over budget"))
    assert build_economic_daily_price_set_v1(**args, maximum_grid_nodes=2)["recommendation_status"] == "QUERY_DOMAIN_UNAVAILABLE"
    stale = replace(args["context"], decision_price_trade_date=date(2025, 1, 27))
    args["context"] = stale
    args["prediction_input"] = args["prediction_input"].model_copy(update={"price_context_sha256": price_context_sha256(stale)})
    result = build_economic_daily_price_set_v1(**args)
    assert result["recommendation_status"] == "UNAVAILABLE" and "RETAINED" in result["reason_code"]
    assert result["prediction_input"]["instrument"] == "000001.SZ"


def test_observed_T_price_consumes_only_the_exact_D_node_and_not_unknown(study):
    args = _daily(study)
    advice = build_economic_daily_price_set_v1(**args)
    query = dict(advice=advice, expected_advice_sha256=advice["advice_sha256"],
                 observation_date=args["prediction_input"].target_date, trading_status="EXECUTABLE")
    assert select_economic_daily_observed_price_v1(**query, observed_price_cny=10.)["action"] == "TAKE"
    assert select_economic_daily_observed_price_v1(**query, observed_price_cny=10.01)["action"] == "SKIP"
    for price in (10.005, 9.2, True, None):
        assert select_economic_daily_observed_price_v1(**query, observed_price_cny=price)["action"] == "UNAVAILABLE"
    assert select_economic_daily_observed_price_v1(**{**query, "trading_status": "SUSPENDED"}, observed_price_cny=None)["action"] == "NOT_APPLICABLE"
    with pytest.raises(AdvisoryModelFirstError, match="exact D advice and T"):
        select_economic_daily_observed_price_v1(**{**query, "observation_date": args["prediction_input"].decision_date}, observed_price_cny=10.)
