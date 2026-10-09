"""Original-D inference, fair pair denominators, unknown cash and source identity."""
import numpy as np
import pytest

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import KEY, POLICY_SHA256
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import ARMS, SCHEMA_SHA256
from backend.services.advisory_model_first.parent_score_price_5td_evaluation_v1 import evaluate_parent_score_cohorts, paired_statistics
from backend.tests.advisory_model_first.test_parent_score_price_5td_inputs_v1 import sample_plan, sample_rows


def forecasts(rows):
    maps = {}
    for arm in ARMS:
        frame = rows.loc[:, ["package_id", "manifest_sha256", "run_id", *KEY]].copy()
        for name, value in dict(status="ACCEPTABLE", expected_net_bps=10., downside_q90_bps=300.,
            profit_probability=.6, model_sha256="a"*64, schema_sha256=SCHEMA_SHA256,
            policy_sha256=POLICY_SHA256, valuation_policy_sha256=VALUATION_POLICY_SHA256).items():
            frame[name] = value
        maps[arm] = frame
    return maps


def test_unknown_cash_pair_denominators_do_not_become_known_action_alpha(tmp_path):
    plan, calendar = sample_plan(tmp_path)
    rows, days = sample_rows(plan, calendar)
    predictions = forecasts(rows)
    selected = rows.instrument.eq("600000.SH")
    predictions[ARMS[1]].loc[selected, "status"] = "UNKNOWN_PARENT_SCORE"
    predictions[ARMS[1]].loc[selected, ["expected_net_bps", "downside_q90_bps", "profit_probability"]] = np.nan
    result = evaluate_parent_score_cohorts(rows=rows, days=days, predictions=predictions, plan=plan, calendar=calendar)
    pair = result["primary"]["pairs"]["baseline"]
    expected = -rows.loc[selected, "hypothetical_liquidation_net_bps"].iloc[0]/5
    assert pair["mean_increment_bps"] == pytest.approx(expected)
    assert pair["attribution"]["known_action_contribution_bps"] == 0
    assert pair["attribution"]["unknown_cash_contribution_bps"] == pytest.approx(expected)
    assert result["activation_evidence"] is False and result["actual_fill_proven"] is False
    # A value hole affects the baseline pair, but two known SKIPs still pair with each other.
    missing = selected & rows[KEY[0]].eq(days.decision_date.iloc[40])
    rows.loc[missing, "hypothetical_liquidation_net_bps"] = np.nan
    rows.loc[missing, ["valuation_gross_terminal_ratio", "valuation_path_min_ratio", "mark_to_market_net_bps"]] = np.nan
    rows.loc[missing, "valuation_status"] = "UNKNOWN"
    for arm in ARMS:
        predictions[arm].loc[missing, ["status", "expected_net_bps"]] = ["AVOID", -1.]
        predictions[arm].loc[missing, ["downside_q90_bps", "profit_probability"]] = [300., .6]
    result = evaluate_parent_score_cohorts(rows=rows, days=days, predictions=predictions, plan=plan, calendar=calendar)
    pairs = result["primary"]["pairs"]
    assert pairs["baseline"]["paired_days"] < pairs[ARMS[0]]["paired_days"]
    assert pairs["baseline"]["simultaneous_95_bonferroni_bps"] is None
    assert pairs[ARMS[0]]["simultaneous_95_bonferroni_bps"] is None


def test_deleted_days_manifest_or_half_package_cannot_improve_result(tmp_path):
    plan, calendar = sample_plan(tmp_path)
    rows, days = sample_rows(plan, calendar)
    predictions = forecasts(rows)
    with pytest.raises(ValueError, match="removed an original"):
        evaluate_parent_score_cohorts(rows=rows, days=days.iloc[:-1], predictions=predictions, plan=plan, calendar=calendar)
    bad = {arm: frame.copy() for arm, frame in predictions.items()}
    bad[ARMS[1]].loc[rows[KEY[0]].ge(str(plan.evaluation_start)), "run_id"] = "other"
    with pytest.raises(ValueError, match="manifest/run"):
        evaluate_parent_score_cohorts(rows=rows, days=days, predictions=bad, plan=plan, calendar=calendar)
    absent = rows.package_id.eq(plan.sources[1].package_id) & rows[KEY[0]].eq(str(plan.evaluation_start))
    days.loc[days.package_id.eq(plan.sources[1].package_id) & days.decision_date.eq(str(plan.evaluation_start)), ["source_status", "original_candidates"]] = ["UNKNOWN_SOURCE_NOT_PROVEN_EMPTY", 0]
    trimmed = {arm: frame.loc[~absent] for arm, frame in predictions.items()}
    result = evaluate_parent_score_cohorts(rows=rows.loc[~absent], days=days, predictions=trimmed, plan=plan, calendar=calendar)
    assert result["primary"]["pairs"]["baseline"]["simultaneous_95_bonferroni_bps"] is None
    assert result["primary"]["pairs"]["baseline"]["paired_days"] == 14


def test_two_endpoints_use_sync_original_blocks_and_family_mde():
    result = paired_statistics(dict(baseline=[float(i) for i in range(20)], matched_core=[float(i*2) for i in range(20)]))
    a, b = result.values()
    assert np.allclose(np.asarray(a["simultaneous_95_bonferroni_bps"])*2, b["simultaneous_95_bonferroni_bps"])
    assert b["mde_80_bps"] == pytest.approx(a["mde_80_bps"]*2)
    assert a["endpoint_alpha"] == .025
    hole = paired_statistics(dict(baseline=[1.]*10+[None]+[1.]*9, matched_core=[2.]*20))
    assert all(v["mde_80_bps"] is None for v in hole.values())
