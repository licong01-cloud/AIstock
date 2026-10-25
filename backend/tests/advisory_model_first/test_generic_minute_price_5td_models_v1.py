"""One shared fitted packet, controlled dimensions and standalone package-independent query."""
from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.generic_minute_price_5td_contracts_v1 import (
    FEATURES, MINUTE_FEATURES, POLICY, POLICY_SHA256, GenericPrice5TDConfigurationV1,
)
from backend.services.advisory_model_first.generic_minute_price_5td_inference_v1 import (
    minute_price_set_5td_v1, query_minute_price_nodes_v1,
)
from backend.services.advisory_model_first.generic_minute_price_5td_models_v1 import (
    GenericMinutePrice5TDFitV1, fitted_identity, matrix_v1, train_generic_minute_price_5td_v1,
)


@pytest.fixture(scope="session")
def minute_fitted_packet():
    days = pd.bdate_range("2024-01-02", periods=25)
    rows = pd.DataFrame([dict(decision_as_of_trade_date=d, target_trade_date=d+pd.Timedelta(days=1),
        instrument=f"{i+1:06d}.SZ", selection_effective_rank=i+1, candidate_group_size=12,
        **{name: .01+i*.001 for name in FEATURES},
        **{name: np.nan if i % 2 else float(i) for name in MINUTE_FEATURES},
        observed_gap_bps=-45.+i*8, gross_terminal_ratio=1.04+i*.001, path_min_ratio=.98,
        label_information_end=d+pd.Timedelta(days=7), label_status="AVAILABLE",
        policy_sha256=POLICY_SHA256, label_contract=POLICY["label_contract"]) for d in days for i in range(12)])
    config = GenericPrice5TDConfigurationV1(train_start="2024-01-01", train_end="2024-03-01",
        validation_start="2024-03-04", validation_end="2024-03-29", test_start="2024-04-01", test_end="2024-04-30", label_cutoff="2024-05-10")
    events = []
    fitted = train_generic_minute_price_5td_v1(rows=rows, configuration=config, before_fit=events.append)
    assert events == ["candidate_mean", "candidate_path", "matched_mean", "matched_path"]
    return fitted, rows, config


def test_four_shared_fits_dimensions_missing_and_package_invariance(minute_fitted_packet):
    fitted, rows, _ = minute_fitted_packet
    assert fitted.diagnostics["training_rows_with_minute_missing"] > 0
    for arm, count in (("candidate", 35), ("matched", 19)):
        assert matrix_v1(rows.iloc[:2], fitted.recipe["medians"], gaps=[0., 0.], arm=arm).shape == (2, count)
    original = query_minute_price_nodes_v1(fitted=fitted, features=rows.iloc[:2], scenario_gap_bps=[0., 0.], arm="candidate")
    for pool in ("stock_universe", "single_index", "index_union"):
        changed = rows.iloc[:2].assign(package_id="ANOTHER", universe_identity=pool, combined_score=999.)
        actual = query_minute_price_nodes_v1(fitted=fitted, features=changed, scenario_gap_bps=[0., 0.], arm="candidate")
        pd.testing.assert_frame_equal(original.iloc[:, :3], actual.iloc[:, :3])
        assert actual.package_id.eq("ANOTHER").all()
    assert original.status.eq("ACCEPTABLE").all()


def test_future_poison_never_changes_weights_medians_or_support(minute_fitted_packet):
    fitted, rows, config = minute_fitted_packet
    poison = rows.iloc[:1].copy()
    poison["decision_as_of_trade_date"], poison["target_trade_date"] = pd.Timestamp("2024-04-01"), pd.Timestamp("2024-04-02")
    poison["label_information_end"] = pd.Timestamp("2024-04-08")
    poison.loc[:, [*FEATURES, *MINUTE_FEATURES]] = np.inf
    poison["observed_gap_bps"], poison["gross_terminal_ratio"] = np.inf, np.inf
    another = train_generic_minute_price_5td_v1(rows=pd.concat([rows, poison]), configuration=config, before_fit=lambda _: None)
    assert another.model_sha256 == fitted.model_sha256


def test_cost_once_full_ticks_holes_and_not_an_open_forecast(minute_fitted_packet):
    fitted, rows, _ = minute_fitted_packet
    heads = deepcopy(fitted.models)
    for name, head in heads.items():
        head["initial"] = 1. if name.endswith("mean") else .99
        for tree in head["trees"]:
            tree["value"] = [0.]*len(tree["value"])
    constant = GenericMinutePrice5TDFitV1(fitted.recipe, heads, fitted.intervals_bps, fitted.diagnostics, "")
    constant = GenericMinutePrice5TDFitV1(constant.recipe, heads, constant.intervals_bps, constant.diagnostics, fitted_identity(constant))
    point = query_minute_price_nodes_v1(fitted=constant, features=rows.iloc[:1], scenario_gap_bps=[0.], arm="candidate").iloc[0]
    assert point.expected_net_bps == pytest.approx(10000*((1-.000595)/(1+.000095)-1)) and point.status == "AVOID"
    values = rows.loc[0, [*FEATURES, *MINUTE_FEATURES]].to_dict()
    result = minute_price_set_5td_v1(fitted=constant, d_features=values, arm="candidate", reference_cny=10.,
                                  legal_low_cny=10., legal_high_cny=10.1, tick_cny=.01)
    assert result["status"] == "UNKNOWN_PARTIAL_OR_NO_ACCEPTABLE" and result["legal_node_count"] == 11
    assert result["unknown_node_count"] > 0 and not result["deployable"]
    empty = minute_price_set_5td_v1(fitted=fitted, d_features=values, arm="candidate", reference_cny=10.,
                                  legal_low_cny=10.001, legal_high_cny=10.009, tick_cny=.01)
    assert empty["status"] == "EMPTY_LEGAL_GRID"


def test_bundle_identity_known_contradiction_and_all_daily_stock_unknown(minute_fitted_packet):
    fitted, rows, _ = minute_fitted_packet
    unknown = rows.iloc[:1].copy()
    unknown.loc[:, FEATURES] = np.nan
    actual = query_minute_price_nodes_v1(fitted=fitted, features=unknown, scenario_gap_bps=[0.], arm="candidate")
    assert actual.status.iloc[0] == "UNKNOWN_INPUT_OR_SUPPORT"
    changed = deepcopy(fitted)
    changed.recipe["candidate_dimensions"] = 19
    with pytest.raises(ValueError, match="identity"):
        query_minute_price_nodes_v1(fitted=changed, features=rows.iloc[:1], scenario_gap_bps=[0.], arm="candidate")
    with pytest.raises(ValueError):
        matrix_v1(rows.iloc[:1], fitted.recipe["medians"], gaps=[-10000.], arm="candidate")
