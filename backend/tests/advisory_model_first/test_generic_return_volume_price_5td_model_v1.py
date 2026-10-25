"""One synthetic fitted packet protects new encoding/price semantics only."""
from copy import deepcopy
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.generic_return_volume_price_5td_contracts_v1 import FEATURES, KEY, LAG_FEATURES, MODEL_FEATURES
from backend.services.advisory_model_first.generic_return_volume_price_5td_model_v1 import (
    fitted_identity, matrix_v1, query_return_volume_nodes_v1, return_volume_price_set_5td_v1, train_return_volume_price_5td_v1,
)
from backend.tests.advisory_model_first.test_generic_price_5td_models_v1 import fitted_packet as fitted_packet


@pytest.fixture(scope="session")
def lag_fitted_packet(fitted_packet):
    control, source, config = fitted_packet
    rows = source.copy()
    for i, k in enumerate(LAG_FEATURES):
        rows[k] = np.linspace(-.01,.01,len(rows)) if i < 2 else np.nan
    events = []
    fitted = train_return_volume_price_5td_v1(rows=rows, frozen_control=control, configuration=config, before_fit=events.append)
    assert events == ["candidate_mean", "candidate_path"]
    return fitted, control, rows, config


def test_two_head_json_and_missing_new_block_does_not_filter_old_availability(lag_fitted_packet):
    fitted, control, rows, _ = lag_fitted_packet
    assert fitted.recipe["medians"][:9] == control.recipe["medians"] and fitted.intervals_bps == control.intervals_bps
    assert fitted.diagnostics["training_keys_sha256"] == control.diagnostics["training_keys_sha256"]
    assert matrix_v1(rows.iloc[:2], fitted.recipe["medians"], [0.,0.]).shape == (2,25)
    absent = rows.iloc[:2].copy()
    absent.loc[:, LAG_FEATURES] = np.nan
    actual = query_return_volume_nodes_v1(fitted=fitted, features=absent, scenario_gap_bps=[0.,0.])
    assert actual.status.ne("UNKNOWN_INPUT_OR_SUPPORT").all()
    for pool in ("stock_universe", "single_index", "index_union"):
        result = query_return_volume_nodes_v1(fitted=fitted, features=absent.assign(package_id="OTHER", universe_identity=pool), scenario_gap_bps=[0.,0.])
        pd.testing.assert_frame_equal(actual[["status","expected_net_bps","downside_q90_bps"]], result[["status","expected_net_bps","downside_q90_bps"]])
    unknown = query_return_volume_nodes_v1(fitted=fitted, features=absent.assign(**{k: np.nan for k in FEATURES}), scenario_gap_bps=[0.,0.])
    assert unknown.status.eq("UNKNOWN_INPUT_OR_SUPPORT").all()


def test_outside_train_poison_cannot_change_fit_or_shared_keys(lag_fitted_packet):
    fitted, control, rows, config = lag_fitted_packet
    poison = rows.iloc[:1].copy()
    poison[KEY[0]], poison[KEY[1]], poison["label_information_end"] = pd.Timestamp("2024-04-01"), pd.Timestamp("2024-04-02"), pd.Timestamp("2024-04-08")
    poison.loc[:, MODEL_FEATURES] = 1e6
    poison["gross_terminal_ratio"], poison["observed_gap_bps"] = 1e6, 1e6
    second = train_return_volume_price_5td_v1(rows=pd.concat([rows,poison]), frozen_control=control, configuration=config, before_fit=lambda _: None)
    assert second.model_sha256 == fitted.model_sha256
    diagnostic = rows.iloc[:1].copy()
    diagnostic[KEY[0]], diagnostic[KEY[1]], diagnostic["label_information_end"] = pd.Timestamp("2024-03-04"), pd.Timestamp("2024-03-05"), pd.Timestamp("2024-03-11")
    checked = train_return_volume_price_5td_v1(rows=pd.concat([rows,diagnostic]), frozen_control=control, configuration=config, before_fit=lambda _: None)
    assert checked.models == fitted.models and checked.recipe == fitted.recipe
    assert checked.diagnostics["validation_diagnostics_only"]["supported_rows"] == 1
    with pytest.raises(ValueError, match="training KEYs"):
        train_return_volume_price_5td_v1(rows=rows.iloc[1:], frozen_control=control, configuration=config, before_fit=lambda _: pytest.fail("must not fit"))


def test_cost_once_complete_tick_holes_and_model_tamper(lag_fitted_packet):
    fitted, _, rows, _ = lag_fitted_packet
    heads = deepcopy(fitted.models)
    for head, body in heads.items():
        body["initial"] = 1. if head == "mean" else .99
        for tree in body["trees"]:
            tree["value"] = [0.]*len(tree["value"])
    constant = replace(fitted, models=heads, model_sha256="")
    constant = replace(constant, model_sha256=fitted_identity(constant))
    result = query_return_volume_nodes_v1(fitted=constant, features=rows.iloc[:1], scenario_gap_bps=[0.]).iloc[0]
    assert result.status == "AVOID" and result.expected_net_bps == pytest.approx(10000*((1-.000595)/(1+.000095)-1))
    args = dict(fitted=constant, d_features=rows.loc[0,list(MODEL_FEATURES)].to_dict(), reference_cny=10., tick_cny=.01)
    output = return_volume_price_set_5td_v1(**args, legal_low_cny=10., legal_high_cny=10.1)
    assert output["status"] == "UNKNOWN_PARTIAL_OR_NO_ACCEPTABLE" and output["legal_node_count"] == 11
    assert return_volume_price_set_5td_v1(**args, legal_low_cny=10.001, legal_high_cny=10.009)["status"] == "EMPTY_LEGAL_GRID"
    heads["mean"]["initial"] = 1.04
    holes = replace(fitted, models=heads, intervals_bps=((-40.,-20.),(20.,40.)), model_sha256="")
    holes = replace(holes, model_sha256=fitted_identity(holes))
    separated = return_volume_price_set_5td_v1(**{**args,"fitted":holes}, legal_low_cny=9.96, legal_high_cny=10.04)
    assert separated["intervals_cny"] == ((9.96,9.98),(10.02,10.04)) and separated["unknown_node_count"] == 3
    broken = deepcopy(fitted)
    broken.recipe["medians"][0] = 99.
    with pytest.raises(ValueError, match="identity"):
        query_return_volume_nodes_v1(fitted=broken, features=rows.iloc[:1], scenario_gap_bps=[0.])
