"""One synthetic six-head fixture; numerical parity is not research evidence."""
from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import original_slot_hurdle_price_5td_model_v1 as core
from backend.services.advisory_model_first import sector_relative_open_price_5td_model_v1 as model
from backend.services.advisory_model_first.sector_relative_open_price_5td_contracts_v1 import (
    ARMS, COMPONENTS, CONDITION_KINDS, CONTEXT_COLUMNS, FEATURES, KEY, MATRIX_ORDERS, ROSTER_KEY, sha,
)


def synthetic_domain():
    rng = np.random.default_rng(31)
    calendar = pd.bdate_range("2024-07-04", periods=100)
    records = []
    for i, d in enumerate(calendar[:60]):
        market = float(10+i%3*20)
        for k in range(5):
            gap = float(rng.uniform(-95, 95))
            value = float(100*rng.gamma(1.5, 2.) if rng.uniform() > .45 else -10000*rng.beta(.8, 8.))
            values = dict(zip(FEATURES, rng.normal(0, .01, len(FEATURES)), strict=True))
            for p in ("p1", "p2"):
                records.append(dict(package_id=p, manifest_sha256="a"*64, run_id=p, source_id=p,
                    **{KEY[0]: d, KEY[1]: calendar[i+1], KEY[2]: f"{600000+k}.SH"}, **values,
                    selection_effective_rank=k+1, candidate_group_size=50, label_information_end=calendar[i+5],
                    label_cluster=sha([str(d), k]), cluster_mass=.5, reference_cny=10., observed_gap_bps=gap,
                    market_open_gap_bps=market, peer_open_median_bps=float(20+k*10), peer_up_fraction=.6,
                    peer_quote_coverage=1., net_return_bps=value, valuation_status="AVAILABLE"))
    clocks = dict(calendar=[str(d.date()) for d in calendar], train_start=str(calendar[0].date()),
        train_end=str(calendar[60].date()))
    return pd.DataFrame(records), clocks


@pytest.fixture(scope="module")
def fitted_native():
    import sklearn.linear_model as linear
    domain, clocks = synthetic_domain()
    encoding = model.fit_encoding_v1(domain, clocks)
    started, models, native = [], {}, []
    logistic, gamma = linear.LogisticRegression, linear.GammaRegressor
    def capture(constructor, **kwargs):
        estimator = constructor(**kwargs)
        native.append(estimator)
        return estimator
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(linear, "LogisticRegression", lambda **kw: capture(logistic, **kw))
        patch.setattr(linear, "GammaRegressor", lambda **kw: capture(gamma, **kw))
        for arm in ARMS:
            models[arm] = {c: model.fit_component_v1(rows=domain, encoding=encoding, arm=arm, component=c,
                plan_sha256="b"*64, before_fit=lambda: started.append(1)) for c in COMPONENTS}
    assert len(started) == 6
    return model.bundle_payload_v1(models=models, encoding=encoding, plan_sha256="b"*64), native


@pytest.fixture(scope="module")
def bundle(fitted_native):
    return fitted_native[0]


def query(bundle, features=None, **overrides):
    domain, _ = synthetic_domain()
    features = domain.iloc[:4].copy() if features is None else features
    args = dict(bundle=bundle, features=features, price_cny=features.reference_cny*(1+features.observed_gap_bps/10000),
        reference_cny=features.reference_cny, market_condition=features[CONTEXT_COLUMNS[0]],
        peer_condition=features[list(CONTEXT_COLUMNS[1:])], condition_kind=CONDITION_KINDS[1], arm=ARMS[1])
    args.update(overrides)
    return model.query_sector_relative_open_nodes_v1(**args)


def test_dimension_order_train_only_encoding_and_h_purge():
    domain, clocks = synthetic_domain()
    encoding = model.fit_encoding_v1(domain, clocks)
    assert [len(MATRIX_ORDERS[a]) for a in ARMS] == [26, 29]
    for arm in ARMS:
        assert model.matrix_v1(domain, domain.observed_gap_bps, encoding, arm).shape == (600, len(MATRIX_ORDERS[arm]))
    changed = domain.assign(net_return_bps=999., label_information_end=pd.Timestamp("2030-01-01"))
    assert model.fit_encoding_v1(changed, clocks) == encoding
    selected, _, weights = model.supervision_v1(domain, encoding)
    assert selected.label_information_end.le(pd.Timestamp(clocks["train_end"])).all()
    assert weights.sum() == selected.label_cluster.nunique()
    with pytest.raises(ValueError, match="evaluation"):
        model.fit_encoding_v1(domain.assign(**{KEY[0]: pd.Timestamp("2030-01-01")}), clocks)


def test_six_head_json_parity_tail_integral_and_package_independence(bundle, fitted_native):
    domain, _ = synthetic_domain()
    rows = domain.iloc[:4]
    result = query(bundle, rows)
    assert result.action.ne("NO_ADVICE").all()
    matrix = model.matrix_v1(rows, rows.observed_gap_bps, bundle["encoding"], ARMS[1])
    values, _ = core.predict_values_v1(bundle["models"][ARMS[1]], matrix)
    native = fitted_native[1]
    np.testing.assert_allclose(result[["p_negative", "p_positive"]], native[2].predict_proba(matrix), atol=1e-12)
    np.testing.assert_allclose(result.positive_mean_bps, 100*native[3].predict(matrix), atol=1e-10)
    np.testing.assert_allclose(result.expected_utility_bps, values[:, -1])
    np.testing.assert_allclose(result.expected_utility_bps, result.expected_net_bps-result.expected_terminal_excess_loss_bps)
    np.testing.assert_allclose(result.iloc[::2].expected_net_bps, result.iloc[1::2].expected_net_bps)
    assert result.calibration_status.eq("UNCALIBRATED").all() and not result.deployable.any()


def test_no_advice_support_unknown_and_bad_known_inputs(bundle):
    domain, _ = synthetic_domain()
    rows = domain.iloc[:4].copy()
    peers = rows[list(CONTEXT_COLUMNS[1:])].copy()
    peers.iloc[0, 0] = np.nan
    peers.iloc[1, 0] = 9999.
    result = query(bundle, rows, peer_condition=peers)
    assert result.action.iloc[:2].eq("NO_ADVICE").all()
    assert result.status.iloc[0] == "UNKNOWN_PEER_SUPPORT" and result.status.iloc[1] == "UNKNOWN_PEER_RANGE"
    assert result.expected_net_bps.iloc[:2].isna().all()
    for arm in ARMS:
        assert query(bundle, rows, peer_condition=peers, arm=arm).action.eq("NO_ADVICE").equals(result.action.eq("NO_ADVICE"))
    with pytest.raises(Exception, match="numeric|finite"):
        query(bundle, rows, market_condition=[float("inf")]*4)
    with pytest.raises(ValueError, match="condition"):
        query(bundle, rows, condition_kind="D_ALREADY_KNOWS_T")
    with pytest.raises(ValueError, match="calendar"):
        query(bundle, rows.assign(**{KEY[1]: rows[KEY[0]]}))
    broken = deepcopy(bundle)
    broken["models"][ARMS[1]]["sign"]["model"]["coef"][0][0] += 1
    with pytest.raises(ValueError, match="hash"):
        query(broken)


def test_complete_conditional_price_set_unknown_holes_and_budget(bundle, monkeypatch):
    domain, _ = synthetic_domain()
    row = domain.iloc[0]
    args = dict(bundle=bundle, d_features=row[list(FEATURES)].to_dict(), source_context=row[list(ROSTER_KEY)].to_dict(),
        reference_cny="10", market_condition=10., peer_condition=row[list(CONTEXT_COLUMNS[1:])].to_dict(),
        condition_kind=CONDITION_KINDS[0], legal_low_cny="9.98", legal_high_cny="10.02", tick_cny=".01", arm=ARMS[1])
    native = model.sector_relative_open_price_set_v1(**dict(args, legal_low_cny="9", legal_high_cny="11"))
    assert native["complete_grid"] and native["original_tick_count"] == 201
    assert native["nodes"][0]["price_cny"] == "9.00" and native["nodes"][-1]["price_cny"] == "11.00"
    # Test the interval assembly independently of a particular fitted model's
    # economic decisions; no mock is used as real prediction acceptance.
    def nodes(**kw):
        n = len(kw["price_cny"])
        result = pd.DataFrame({k: np.ones(n) for k in model.VALUE_FIELDS})
        result["action"] = ["TAKE", "NO_ADVICE", "TAKE", "SKIP", "TAKE"][:n]
        result["status"] = "SYNTHETIC_INTERVAL_CONTRACT"
        return result
    monkeypatch.setattr(model, "query_sector_relative_open_nodes_v1", nodes)
    result = model.sector_relative_open_price_set_v1(**args)
    assert result["complete_grid"] and result["original_tick_count"] == 5
    assert result["intervals_cny"] == [["9.98", "9.98"], ["10.00", "10.00"], ["10.02", "10.02"]]
    assert result["unknown_nodes"] == 1 and not result["deployable"]
    with pytest.raises(ValueError, match="budget"):
        model.sector_relative_open_price_set_v1(**dict(args, legal_high_cny="2000"))
