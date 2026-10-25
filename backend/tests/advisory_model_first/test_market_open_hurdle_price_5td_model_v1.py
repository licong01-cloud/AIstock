"""The new market matrix/condition/grid, not another test of shared Beta math."""
from copy import deepcopy

import numpy as np
import pytest
from threadpoolctl import threadpool_limits

from backend.tests.advisory_model_first.test_market_open_hurdle_price_5td_inputs_v1 import prepared_study as prepared_study
from backend.services.advisory_model_first import market_open_hurdle_price_5td_model_v1 as model
from backend.services.advisory_model_first.market_open_hurdle_price_5td_contracts_v1 import (
    ARMS, CLOCK, COMPONENTS, FEATURES, MARKET_COLUMN, ROSTER_KEY, sha,
)


@pytest.fixture(scope="session")
def fitted_study(prepared_study):
    plan, p, _ = prepared_study
    with threadpool_limits(limits=2):
        models = {a: {c: model.fit_component_v1(rows=p["domain"], encoding=p["encoding"], arm=a, component=c,
            plan_sha256=plan.plan_sha256) for c in COMPONENTS} for a in ARMS}
    return model.bundle_payload_v1(models=models, encoding=p["encoding"], plan_sha256=plan.plan_sha256)


def query(bundle, features, arm, *, market=None, kind="HYPOTHETICAL_MARKET_CONDITION"):
    return model.query_hurdle_nodes_v1(bundle=bundle, features=features,
        scenario_gap_bps=features.observed_gap_bps,
        scenario_market_gap_bps=features[MARKET_COLUMN] if market is None else market,
        market_condition_kind=kind, arm=arm)


def test_new_coordinate_and_native_prediction_parity(prepared_study, monkeypatch):
    from sklearn.linear_model import GammaRegressor, LogisticRegression
    plan, p, _ = prepared_study
    captured, logit, gamma = {}, LogisticRegression.fit, GammaRegressor.fit
    def capture_sign(self, *args, **kwargs):
        result = logit(self, *args, **kwargs)
        captured["sign"] = self
        return result
    def capture_positive(self, *args, **kwargs):
        result = gamma(self, *args, **kwargs)
        captured["positive"] = self
        return result
    monkeypatch.setattr(LogisticRegression, "fit", capture_sign)
    monkeypatch.setattr(GammaRegressor, "fit", capture_positive)
    rows, _, _ = model.supervision_v1(p["domain"], p["encoding"])
    np.testing.assert_allclose(model.market_basis_v1([300], [100]), [[1, 3, 2]])
    for arm, width in zip(ARMS, (22, 25), strict=True):
        with threadpool_limits(limits=2):
            heads = {c: model.fit_component_v1(rows=rows, encoding=p["encoding"], arm=arm, component=c,
                plan_sha256=plan.plan_sha256) for c in COMPONENTS}
        matrix = model.matrix_v1(rows, rows.observed_gap_bps, p["encoding"], arm)
        assert matrix.shape == (len(rows), width)
        predicted, _ = model.predict_values_v1(heads, matrix)
        np.testing.assert_allclose(predicted[:, 0], captured["sign"].predict_proba(matrix)[:, -1], atol=1e-12)
        np.testing.assert_allclose(predicted[:, 3], captured["positive"].predict(matrix)*100, atol=1e-10)


def test_explicit_condition_and_common_support_preserve_unknown(prepared_study, fitted_study):
    _, p, _ = prepared_study
    rows = p["domain"].iloc[:3].copy()
    for arm in ARMS:
        unknown = query(fitted_study, rows, arm, market=[None, 9999, rows[MARKET_COLUMN].iloc[2]])
        assert unknown.status.iloc[:2].tolist() == ["UNKNOWN_MARKET_OPEN", "UNKNOWN_MARKET_SUPPORT"]
        assert unknown.information_clock.eq(CLOCK).all() and not unknown.market_open_known_at_D.any()
        assert unknown.iloc[:2].expected_utility_bps.isna().all()
    with pytest.raises(ValueError):
        query(fitted_study, rows, ARMS[0], kind="D_KNOWN_T_FUTURE")


def test_shared_stock_price_condition_across_packages_and_immutable_bundle(prepared_study, fitted_study):
    _, p, _ = prepared_study
    row = p["domain"].iloc[[0]].copy()
    other = row.copy()
    other["package_id"], other["run_id"], other["manifest_sha256"] = "different", "run", "f"*64
    for arm in ARMS:
        a, b = query(fitted_study, row, arm), query(fitted_study, other, arm)
        assert a.expected_utility_bps.iloc[0] == b.expected_utility_bps.iloc[0]
    broken = deepcopy(fitted_study)
    broken["models"][ARMS[1]]["sign"]["model"]["coef"][0].pop()
    head = broken["models"][ARMS[1]]["sign"]
    head["component_sha256"] = sha({n: v for n, v in head.items() if n != "component_sha256"})
    with pytest.raises(ValueError):
        model.validate_bundle_v1(broken)


def test_market_condition_is_a_real_input_only_to_new_arm(prepared_study, fitted_study):
    _, p, _ = prepared_study
    scenario = deepcopy(fitted_study)
    head = scenario["models"][ARMS[1]]["positive"]
    head["model"]["coef"][-3] = .1
    head["component_sha256"] = sha({n: v for n, v in head.items() if n != "component_sha256"})
    scenario = model.bundle_payload_v1(models=scenario["models"], encoding=scenario["encoding"], plan_sha256=scenario["plan_sha256"])
    row = p["domain"].iloc[[0]]
    lo, hi = (scenario["encoding"][n] for n in ("market_min_bps", "market_max_bps"))
    for arm in ARMS:
        a, b = query(scenario, row, arm, market=[lo]), query(scenario, row, arm, market=[hi])
        assert a.expected_utility_bps.notna().all() and b.expected_utility_bps.notna().all()
        assert (a.expected_utility_bps.iloc[0] == b.expected_utility_bps.iloc[0]) == (arm == ARMS[0])


def test_all_missing_market_has_no_fake_fit_or_fallback(prepared_study):
    _, p, _ = prepared_study
    rows = p["domain"].copy()
    rows[MARKET_COLUMN] = np.nan
    with pytest.raises(ValueError, match="PREPARED_NO_FIT"):
        model.supervision_v1(rows, p["encoding"])


def test_full_legal_tick_grid_keeps_market_unknown_and_explicit_scenario(prepared_study, fitted_study):
    _, p, _ = prepared_study
    row = p["domain"].iloc[0]
    args = dict(bundle=fitted_study, d_features={n: row[n] for n in FEATURES},
        source_context={n: row[n] for n in ROSTER_KEY}, reference_cny="10.00",
        legal_low_cny="9.99", legal_high_cny="10.01", tick_cny="0.01", arm=ARMS[1],
        scenario_market_gap_bps=None, market_condition_kind="HYPOTHETICAL_MARKET_CONDITION")
    result = model.hurdle_price_set_v1(**args)
    assert result["complete_grid"] and result["original_tick_count"] == 3 and result["unknown_nodes"] == 3
    assert result["status"] == "UNKNOWN" and result["intervals_cny"] == []
    assert result["market_condition_kind"] == args["market_condition_kind"] and not result["market_open_known_at_D"]
    assert [n["price_cny"] for n in result["nodes"]] == ["9.99", "10.00", "10.01"]
    args.update(legal_low_cny="10.001", legal_high_cny="10.009", scenario_market_gap_bps=0.)
    empty = model.hurdle_price_set_v1(**args)
    assert empty["status"] == "EMPTY_LEGAL_GRID" and empty["original_tick_count"] == 0
