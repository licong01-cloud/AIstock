"""One tiny synthetic unit fit, not a registered research/economic experiment."""
import numpy as np
import pandas as pd
import pytest

from backend.tests.advisory_model_first.test_risk_tail_calibrated_price_5td_inputs_v1 import synthetic_study as synthetic_study
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES
from backend.services.advisory_model_first.generic_population_price_5td_model_v1 import weighted_quantile_v1
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_contracts_v1 import ARMS, ROSTER_KEY
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_inputs_v1 import CAL_FINANCE, read_projection_v1
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_model_v1 import (
    bundle_from_payload_v1, bundle_payload_v1, core_from_payload_v1, core_payload_v1, fit_shared_core_v1,
    fit_tail_residual_calibration_v1, query_risk_tail_nodes_v1, risk_tail_price_set_v1,
)


@pytest.fixture(scope="session")
def fitted_study(synthetic_study):
    plan, prepared, _ = synthetic_study
    observations = []
    core = fit_shared_core_v1(rows=prepared["base"], encoding=prepared["encoding"], plan_sha256=plan.plan_sha256,
        before_fit=lambda a: observations.append(("before", a)), after_fit=lambda a: observations.append(("after", a)))
    assert observations == [("before", ARMS[0]), ("after", ARMS[0])]
    cal = read_projection_v1(plan=plan, dates=prepared["encoding"]["calibration_dates"], columns=CAL_FINANCE, top5=True)
    card = fit_tail_residual_calibration_v1(core=core, calibration_rows=cal,
        original_calibration_roster=prepared["calibration_roster"], plan_sha256=plan.plan_sha256)
    return core, card, cal


@pytest.mark.parametrize("lower,sign", [(.985, -1), (.88, 0), (.80, 1)])
def test_training_only_delta_signs_and_unchanged_profit_head(synthetic_study, fitted_study, lower, sign):
    plan, prepared, _ = synthetic_study
    core, _, cal = fitted_study
    cal = cal.assign(valuation_path_min_ratio=lower)
    card = fit_tail_residual_calibration_v1(core=core, calibration_rows=cal,
        original_calibration_roster=prepared["calibration_roster"], plan_sha256=plan.plan_sha256)
    assert np.sign(card["delta"]) == sign
    assert card["known_rows"] == len(cal) and card["known_original_mass"] == pytest.approx(1.)
    bundle = bundle_payload_v1(core, card)
    features = cal[[*ROSTER_KEY, *FEATURES]].iloc[:2]
    raw, candidate = [query_risk_tail_nodes_v1(bundle=bundle, features=features, scenario_gap_bps=[0., 0.], arm=a) for a in ARMS]
    assert raw.status.eq("AVOID").all()  # Calibration includes the AVOID population.
    assert np.array_equal(raw.expected_net_bps, candidate.expected_net_bps)
    assert np.array_equal(raw.profit_probability, candidate.profit_probability)
    assert np.allclose(candidate.downside_q90_bps, np.clip(raw.downside_q90_bps+card["delta"], 0, 10000))
    assert card["effective_raw_cutoff"] == 800-card["delta"]
    assert core_from_payload_v1(core_payload_v1(core)).model_sha256 == core.model_sha256
    assert weighted_quantile_v1([100, 200, 300], [.85, .05, .10], .9) == 200


def test_empty_calibration_is_unknown_not_zero_or_raw_fallback(synthetic_study, fitted_study):
    plan, prepared, _ = synthetic_study
    core, _, cal = fitted_study
    card = fit_tail_residual_calibration_v1(core=core, calibration_rows=cal.assign(valuation_status="UNKNOWN", valuation_path_min_ratio=np.nan),
        original_calibration_roster=prepared["calibration_roster"], plan_sha256=plan.plan_sha256)
    assert card["delta"] is None and card["calibrated_parameter_count"] == 0
    result = query_risk_tail_nodes_v1(bundle=bundle_payload_v1(core, card), features=cal[[*ROSTER_KEY, *FEATURES]].iloc[:1],
        scenario_gap_bps=[0], arm=ARMS[1])
    assert result.status.tolist() == ["UNKNOWN_RISK_CALIBRATION"]
    assert result.downside_q90_bps.isna().all() and result.expected_net_bps.notna().all()


@pytest.mark.parametrize("field", ["cal_weight", "policy", "core", "calibration"])
def test_payload_and_calibration_identity_tampering_is_rejected(synthetic_study, fitted_study, field):
    plan, prepared, _ = synthetic_study
    core, card, cal = fitted_study
    if field == "cal_weight":
        roster = prepared["calibration_roster"].copy()
        roster.loc[roster.index[0], "original_weight"] *= 2
        with pytest.raises(ValueError, match="identity"):
            fit_tail_residual_calibration_v1(core=core, calibration_rows=cal, original_calibration_roster=roster, plan_sha256=plan.plan_sha256)
        return
    value = bundle_payload_v1(core, card)
    if field == "policy":
        value["core"]["recipe"]["policy_sha256"] = "a"*64
    elif field == "core":
        value["core"]["forest"][0]["threshold"][0] += 1
    else:
        value["tail_calibration"]["delta"] = 0
    with pytest.raises(ValueError, match="identity"):
        bundle_from_payload_v1(value)


def test_decimal_price_grid_preserves_unknown_holes_and_empty_grid(fitted_study):
    core, card, cal = fitted_study
    row = cal.iloc[0]
    query = dict(bundle=bundle_payload_v1(core, card), d_features={n: None if pd.isna(row[n]) else row[n] for n in FEATURES},
        source_context={n: row[n] for n in ROSTER_KEY}, reference_cny="10", legal_low_cny="9.99", legal_high_cny="10.01",
        tick_cny=".01", arm=ARMS[1])
    result = risk_tail_price_set_v1(**query)
    assert result["original_tick_count"] == 3 and result["unknown_nodes"] == 2
    assert result["intervals_cny"] == [["10.00", "10.00"]]
    assert result["nodes"][0]["expected_net_bps"] is None
    assert result["nodes"][1]["downside_q90_bps"] < result["nodes"][1]["uncalibrated_downside_q90_bps"]
    empty = risk_tail_price_set_v1(**dict(query, legal_low_cny="10.001", legal_high_cny="10.009"))
    assert empty["empty_legal_grid"] and empty["nodes"] == []
    with pytest.raises(ValueError, match="coordinates"):
        risk_tail_price_set_v1(**dict(query, reference_cny="1e1000"))


@pytest.mark.parametrize("case,reason", [("stock", "UNKNOWN_STOCK_INPUT"), ("package", "UNKNOWN_PACKAGE_ADAPTER"),
    ("gap", "UNKNOWN_PRICE_SCENARIO"), ("support", "UNKNOWN_GAP_SUPPORT")])
def test_typed_unknowns_do_not_fabricate_profit(fitted_study, case, reason):
    core, card, cal = fitted_study
    row = cal[[*ROSTER_KEY, *FEATURES]].iloc[:1].copy()
    gap = 0.
    if case == "stock":
        row.loc[:, FEATURES] = np.nan
    elif case == "package":
        row.loc[:, "run_id"] = "foreign"
    else:
        gap = np.nan if case == "gap" else 900.
    result = query_risk_tail_nodes_v1(bundle=bundle_payload_v1(core, card), features=row, scenario_gap_bps=[gap], arm=ARMS[1])
    assert result.status.tolist() == [reason]
    assert result[["expected_net_bps", "profit_probability", "downside_q90_bps"]].isna().all().all()
