"""Scenario clock/units/support contracts, without research fits or snapshots."""
import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first import generic_exit_price_conditioned_5td_models_v1 as m
from backend.services.advisory_model_first.generic_exit_price_conditioned_5td_contracts_v1 import (
    FEATURES, KEY, RECIPE_SHA256, S_FIELDS, TRAIN_FIELDS,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def state(episode="original", value=.01, remaining=.5):
    return dict(episode_id=episode, s_date="2025-01-06", **dict.fromkeys(FEATURES, value), remaining_session_fraction=remaining)


def frozen_model():
    encoding = dict(medians=[0.]*11, mean=[0.]*11, scale=[1.]*11)
    body = dict(status="FITTED", physical_fits=1, ordinal=1, recipe_sha256=RECIPE_SHA256,
        encoding=encoding, coefficients=[0.]*10+[5000.]+[0.]*9, intercept=10., support={"2": [[-100., 400.]]})
    return {**body, "model_sha256": sha(body)}


def test_twenty_inputs_train_only_encoding_unknown_flags_and_scenario_role():
    train = pd.DataFrame([state("a", .01), state("b", .03)])
    train[FEATURES[8]] = np.nan
    x, recipe = m._matrix(train, [-100., 100.])
    future, same = m._matrix(pd.DataFrame([state("future", 1e20)]), [300.], recipe)
    assert x.shape == (2, 20) and future.shape == (1, 20) and same == recipe
    assert recipe["medians"][0] == .02 and recipe["medians"][8] == 0.
    assert np.all(x[:, 19] == 1.) and future[0, 0] > 1e10


def test_s_curve_query_changes_value_but_never_double_charges_fees():
    curves = m.seal_s_curves_v1(model=frozen_model(), s_inputs=pd.DataFrame([state()]))
    for query, hold in ((100., 60.), (200., 110.)):
        predictions = m.query_sealed_curves_v1(curves=curves, queries=pd.DataFrame([dict(episode_id="original", s_date="2025-01-06", sell_scenario_bps=query)]))
        assert predictions.predicted_y_hold_bps.iloc[0] == pytest.approx(hold)
        assert query-predictions.predicted_y_hold_bps.iloc[0] == pytest.approx(query-hold)


def test_future_label_poison_cannot_change_sealed_curve_and_is_rejected_as_s_input():
    full = pd.DataFrame([{**state(), "sell_scenario_bps": 100., "continue_net_cny": 1.03, "u_factor": 2., "e_close": 100.}])
    before = m.seal_s_curves_v1(model=frozen_model(), s_inputs=full.loc[:, S_FIELDS])
    full.loc[:, ["sell_scenario_bps", "continue_net_cny", "u_factor", "e_close"]] = 1e20
    after = m.seal_s_curves_v1(model=frozen_model(), s_inputs=full.loc[:, S_FIELDS])
    assert before.curve_sha256.tolist() == after.curve_sha256.tolist()
    with pytest.raises(ValueError, match="S-only"):
        m.seal_s_curves_v1(model=frozen_model(), s_inputs=full)


@pytest.mark.parametrize("query,status", [(None, "UNKNOWN_QUERY_VALUE"), (np.nan, "UNKNOWN_QUERY_VALUE"), (500., "UNKNOWN_QUERY_SUPPORT")])
def test_normal_missing_and_unsupported_queries_keep_original_keys(query, status):
    curves = m.seal_s_curves_v1(model=frozen_model(), s_inputs=pd.DataFrame([state()]))
    result = m.query_sealed_curves_v1(curves=curves, queries=pd.DataFrame([dict(episode_id="original", s_date="2025-01-06", sell_scenario_bps=query)]))
    assert result.episode_id.tolist() == ["original"] and result.query_status.iloc[0] == status
    assert pd.isna(result.predicted_y_hold_bps.iloc[0])


def test_support_preserves_holes_and_remainings_are_not_pooled():
    rows = []
    for fraction, values in ((.5, [20., 520.]), (.25, [220.])):
        for value in values:
            rows.extend(dict(remaining_session_fraction=fraction, sell_scenario_bps=value,
                entry_date="2025-01-0"+str(1+i%5)) for i in range(40))
    support = m._support(pd.DataFrame(rows))
    assert support["2"] == [[20., np.nextafter(100., -np.inf)], [500., 520.]]
    assert support["1"] == [[220., 220.]]
    assert not any(low <= 220. <= high for low, high in support["2"])


def test_recipe_fit_is_one_new_candidate_and_empty_train_is_zero_fit(monkeypatch):
    steps = []
    class FakeRidge:
        def __init__(self, alpha):
            assert alpha == 1.
        def fit(self, x, y):
            assert steps == ["before"] and x.shape == (2, 20) and list(y) == [10., 20.]
            self.coef_, self.intercept_ = np.zeros(20), 0.
            steps.append("fit")
            return self
        def predict(self, x):
            return np.zeros(len(x))
    monkeypatch.setattr(m, "Ridge", FakeRidge)
    train = pd.DataFrame([{**state(str(i)), "entry_date": "2025-01-02", "held": True,
        "y_hold_bps": 10.*(i+1), "sell_scenario_bps": 100.*i} for i in range(2)], columns=TRAIN_FIELDS)
    fitted = m.fit_price_conditioned_fold_v1(train_rows=train, ordinal=1, before_fit=lambda ordinal: steps.append("before"))
    assert steps == ["before", "fit"] and fitted["physical_fits"] == 1
    train["sell_scenario_bps"] = None
    unknown = m.fit_price_conditioned_fold_v1(train_rows=train, ordinal=1, before_fit=lambda ordinal: pytest.fail("no known query"))
    curves = m.seal_s_curves_v1(model=unknown, s_inputs=train.loc[:, S_FIELDS])
    predictions = m.query_sealed_curves_v1(curves=curves, queries=train.loc[:, [*KEY, "sell_scenario_bps"]])
    assert unknown["physical_fits"] == 0 and predictions.query_status.eq("UNKNOWN_NO_MATURE_QUERY_TRAIN").all()


def test_sealed_identity_unknown_numeric_and_foreign_query_are_not_silent_fallback():
    curves = m.seal_s_curves_v1(model=frozen_model(), s_inputs=pd.DataFrame([state()]))
    query = pd.DataFrame([dict(episode_id="original", s_date="2025-01-06", sell_scenario_bps=10.)])
    curves.loc[0, "a_bps"] += 1
    with pytest.raises(ValueError, match="identity"):
        m.query_sealed_curves_v1(curves=curves, queries=query)
    with pytest.raises(AdvisoryModelFirstError, match="booleans"):
        m._matrix(pd.DataFrame([state(value=True)]), [100.])
