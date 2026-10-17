"""One fitted fixture; different metadata never changes shared price mathematics."""
from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import (
    FEATURES, POLICY, POLICY_SHA256, GenericPrice5TDConfigurationV1,
)
from backend.services.advisory_model_first.generic_price_5td_inference_v1 import (
    generic_price_set_5td_v1, query_price_nodes_v1,
)
from backend.services.advisory_model_first.generic_price_5td_models_v1 import (
    GenericPrice5TDFitV1, fitted_identity, matrix, train_generic_price_5td_v1,
)


@pytest.fixture(scope="session")
def fitted_packet():
    days = pd.bdate_range("2024-01-02", periods=32)
    rows = []
    for d in days[:25]:
        for i in range(12):
            rows.append(dict(decision_as_of_trade_date=d, target_trade_date=d+pd.Timedelta(days=1),
                instrument=f"{i+1:06d}.SZ", **{name: .01+i*.001 for name in FEATURES},
                observed_gap_bps=-45.+i*8, gross_terminal_ratio=1.04+i*.001, path_min_ratio=.98,
                label_information_end=d+pd.Timedelta(days=7), label_status="AVAILABLE",
                policy_sha256=POLICY_SHA256, label_contract=POLICY["label_contract"]))
    frame = pd.DataFrame(rows)
    frame["market_up_ratio"] = np.nan
    config = GenericPrice5TDConfigurationV1(train_start="2024-01-01", train_end="2024-03-01",
        validation_start="2024-03-04", validation_end="2024-03-29", test_start="2024-04-01",
        test_end="2024-04-30", label_cutoff="2024-05-10")
    events = []
    fitted = train_generic_price_5td_v1(rows=frame, configuration=config, before_fit=events.append)
    assert events == ["candidate_mean", "candidate_path", "matched_mean", "matched_path"]
    return fitted, frame, config


def test_real_four_JSON_fits_missing_encoding_and_package_metadata(fitted_packet):
    fitted, rows, _ = fitted_packet
    assert fitted.diagnostics["physical_fit_count"] == 4
    assert fitted.recipe["medians"][-1] == 0 and not fitted.diagnostics["test_used_for_training_or_calibration"]
    x = matrix(rows.iloc[:2], fitted.recipe["medians"])
    assert x.shape == (2, 18) and np.all(x[:, -1] == 1)
    original = query_price_nodes_v1(fitted=fitted, features=rows.iloc[:2], scenario_gap_bps=[0., 0.], arm="candidate")
    for pool in ("stock_universe", "single_index", "index_union"):
        modified = rows.iloc[:2].assign(package_id="OTHER", universe_identity=pool, combined_score=999., selection_effective_rank=99)
        actual = query_price_nodes_v1(fitted=fitted, features=modified, scenario_gap_bps=[0., 0.], arm="candidate")
        pd.testing.assert_frame_equal(original[["status", "expected_net_bps", "downside_q90_bps"]],
                                      actual[["status", "expected_net_bps", "downside_q90_bps"]])
        assert actual.package_id.eq("OTHER").all() and actual.universe_identity.eq(pool).all()
    assert original.status.eq("ACCEPTABLE").all()
    assert original.policy_sha256.eq(POLICY_SHA256).all()
    assert all("market_up_ratio" in value for value in original.input_unknown_fields)


def test_test_poison_never_changes_training_medians_support_or_models(fitted_packet):
    fitted, rows, config = fitted_packet
    poison = rows.iloc[:1].copy()
    poison["decision_as_of_trade_date"] = pd.Timestamp("2024-04-01")
    poison["target_trade_date"] = pd.Timestamp("2024-04-02")
    poison["label_information_end"] = pd.Timestamp("2024-04-08")
    poison.loc[:, FEATURES] = 1e6
    poison["observed_gap_bps"], poison["gross_terminal_ratio"] = 1e6, 1e6
    second = train_generic_price_5td_v1(rows=pd.concat([rows, poison]), configuration=config, before_fit=lambda _: None)
    assert second.model_sha256 == fitted.model_sha256


def test_complete_tick_unknown_holes_no_fake_empty_or_fill(fitted_packet):
    fitted, rows, _ = fitted_packet
    features = rows.loc[0, list(FEATURES)].to_dict()
    result = generic_price_set_5td_v1(fitted=fitted, d_features=features, arm="candidate", reference_cny=10.,
        legal_low_cny=9.9, legal_high_cny=10.1, tick_cny=.01)
    assert result["legal_node_count"] == 21 and result["unknown_node_count"] > 0
    assert result["status"] == "ACCEPTABLE_PRICE_SET" and not result["deployable"]
    unknown = generic_price_set_5td_v1(fitted=fitted, d_features=features, arm="candidate", reference_cny=10.,
        legal_low_cny=8., legal_high_cny=9., tick_cny=.01)
    assert unknown["status"] == "UNKNOWN_INPUT_OR_SUPPORT" and unknown["intervals_cny"] == ()
    no_ticks = generic_price_set_5td_v1(fitted=fitted, d_features=features, arm="candidate", reference_cny=10.,
        legal_low_cny=10.001, legal_high_cny=10.009, tick_cny=.01)
    assert no_ticks["status"] == "EMPTY_LEGAL_GRID"


def test_unknown_stock_and_hash_tamper_distinct_from_bad_known_input(fitted_packet):
    fitted, rows, _ = fitted_packet
    absent = rows.iloc[:1].copy()
    absent.loc[:, FEATURES] = np.nan
    output = query_price_nodes_v1(fitted=fitted, features=absent, scenario_gap_bps=[0.], arm="matched")
    assert output.status.iloc[0] == "UNKNOWN_INPUT_OR_SUPPORT"
    altered = deepcopy(fitted)
    altered.recipe["medians"][0] = 999.
    with pytest.raises(ValueError, match="identity"):
        query_price_nodes_v1(fitted=altered, features=rows.iloc[:1], scenario_gap_bps=[0.], arm="candidate")
    for gap in (True, float("inf"), -10000.):
        with pytest.raises((ValueError, AdvisoryModelFirstError)):
            query_price_nodes_v1(fitted=fitted, features=rows.iloc[:1], scenario_gap_bps=[gap], arm="candidate")


def test_cost_once_hand_point_negative_net_and_partial_unknown_not_no_price(fitted_packet):
    fitted, rows, _ = fitted_packet
    heads = deepcopy(fitted.models)
    for name, model in heads.items():
        model["initial"] = 1. if name.endswith("mean") else .99
        for tree in model["trees"]:
            tree["value"] = [0.]*len(tree["value"])
    adjusted = GenericPrice5TDFitV1(fitted.recipe, heads, fitted.intervals_bps, fitted.diagnostics, "")
    adjusted = GenericPrice5TDFitV1(adjusted.recipe, heads, adjusted.intervals_bps, adjusted.diagnostics, fitted_identity(adjusted))
    point = query_price_nodes_v1(fitted=adjusted, features=rows.iloc[:1], scenario_gap_bps=[0.], arm="candidate").iloc[0]
    assert point.expected_net_bps == pytest.approx(10000*((1-.000595)/(1+.000095)-1))
    assert point.status == "AVOID" and point.downside_q90_bps == pytest.approx(100.)
    result = generic_price_set_5td_v1(fitted=adjusted, d_features=rows.loc[0, list(FEATURES)].to_dict(), arm="candidate",
        reference_cny=10., legal_low_cny=10., legal_high_cny=10.1, tick_cny=.01)
    assert result["status"] == "UNKNOWN_PARTIAL_OR_NO_ACCEPTABLE"
