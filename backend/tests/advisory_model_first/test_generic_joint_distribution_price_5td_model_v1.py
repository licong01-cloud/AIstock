"""Shared minimal fixture and exact joint-distribution/clock/price contracts."""
from copy import deepcopy
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.generic_joint_distribution_price_5td_model_v1 import (
    fitted_identity, joint_distribution_v1,
    joint_price_set_5td_v1, query_joint_price_nodes_v1, train_joint_price_5td_v1,
)
from backend.services.advisory_model_first.generic_volume_path_price_5td_contracts_v1 import FEATURES, MINUTE_FEATURES
from backend.services.advisory_model_first.generic_volume_path_price_5td_contracts_v1 import POLICY
from backend.services.advisory_model_first.generic_volume_path_price_5td_model_v1 import matrix_v1
from backend.tests.advisory_model_first.test_generic_volume_path_price_5td_model_v1 import volume_fitted_packet as volume_fitted_packet


@pytest.fixture(scope="session")
def joint_fitted_packet(volume_fitted_packet):
    control, rows, config = volume_fitted_packet
    events = []
    fitted = train_joint_price_5td_v1(rows=rows, frozen_control=control, configuration=config, before_fit=events.append)
    assert events == ["candidate_joint_forest"]
    return fitted, control, rows, config


def test_hand_weights_joint_quantile_no_tree_pseudoreplication_and_empty_mass():
    split = dict(feature=[0, -2, -2], threshold=[0., -2., -2.], left=[1, -1, -1], right=[2, -1, -1],
                 members={"1": [0, 1], "2": [1, 2]})
    flat = dict(feature=[-2], threshold=[-2.], left=[-1], right=[-1], members={"0": [1, 2]})
    x = np.zeros((1, 39))
    x[:, 0] = -1.
    calibration = dict(terminal=[.95, 1.05, 1.15], path=[.9, .98, 1.01])
    weights, mean, lower, trees = joint_distribution_v1(forest=(split, flat), calibration=calibration, matrix=x)
    np.testing.assert_allclose(weights, [[.25, .5, .25]])
    assert mean[0] == pytest.approx(1.05) and lower[0] == .9 and trees[0] == 2
    doubled = joint_distribution_v1(forest=(split, flat, split, flat), calibration=calibration, matrix=x)
    np.testing.assert_allclose(doubled[0], weights)
    empty = deepcopy(flat)
    empty["members"] = {}
    unknown = joint_distribution_v1(forest=(empty,), calibration=calibration, matrix=x)
    assert unknown[0].sum() == 0 and np.isnan(unknown[1][0]) and np.isnan(unknown[2][0])
    with pytest.raises(ValueError, match="batch"):
        joint_distribution_v1(forest=(flat,), calibration=calibration, matrix=np.zeros((129, 39)))


def test_honest_roles_real_JSON_parity_cost_once_and_cross_package_query(joint_fitted_packet):
    fitted, _, rows, _ = joint_fitted_packet
    diag = fitted.diagnostics
    assert diag["physical_fit_count"] == 1 and diag["internal_tree_count"] == 128
    assert diag["structure_latest_label_end"] < diag["estimation_start"]
    assert diag["structure_rows"]+diag["estimation_rows"] < diag["original_mature_pool_rows"]
    assert not diag["test_used_for_training_or_calibration"]
    original = query_joint_price_nodes_v1(fitted=fitted, features=rows.iloc[:2], scenario_gap_bps=[0., 0.])
    assert original.status.eq("ACCEPTABLE").all() and original.distribution_known_trees.gt(0).all()
    for pool in ("stock_universe", "single_index", "index_union"):
        changed = query_joint_price_nodes_v1(fitted=fitted,
            features=rows.iloc[:2].assign(package_id="NEW", universe_identity=pool, combined_score=999.),
            scenario_gap_bps=[0., 0.])
        pd.testing.assert_frame_equal(original.iloc[:, :6], changed.iloc[:, :6])
    assert original.profit_probability.between(0, 1).all()
    matrix = matrix_v1(rows.iloc[:1], fitted.recipe["medians"], gaps=[0.], arm="candidate")
    _, mean, _, _ = joint_distribution_v1(forest=fitted.forest, calibration=fitted.calibration, matrix=matrix)
    assert original.expected_net_bps.iloc[0] == pytest.approx(
        10000*(mean[0]*(1-POLICY["sell_bps"]/10000)/(1+POLICY["buy_bps"]/10000)-1))
    assert original.weight_effective_sample_size.gt(0).all()
    assert not original.effective_sample_size_is_time_independence.any()


def test_future_poison_and_mature_pool_identity(joint_fitted_packet):
    fitted, control, rows, config = joint_fitted_packet
    poison = rows.iloc[:1].copy()
    poison["decision_as_of_trade_date"], poison["target_trade_date"] = pd.Timestamp("2024-04-01"), pd.Timestamp("2024-04-02")
    poison.loc[:, [*FEATURES, *MINUTE_FEATURES, "gross_terminal_ratio"]] = np.inf
    poison["label_information_end"] = "not parsed outside training"
    another = train_joint_price_5td_v1(rows=pd.concat([rows, poison]), frozen_control=control,
                                     configuration=config, before_fit=lambda _: None)
    assert another.model_sha256 == fitted.model_sha256
    changed = rows.copy()
    changed.loc[0, "label_status"] = "UNKNOWN"
    with pytest.raises(ValueError, match="mature pool"):
        train_joint_price_5td_v1(rows=changed, frozen_control=control, configuration=config, before_fit=lambda _: None)


def test_unknown_mass_and_full_legal_tick_holes(joint_fitted_packet):
    fitted, _, rows, _ = joint_fitted_packet
    empty = deepcopy(fitted)
    for tree in empty.forest:
        tree["members"] = {}
    empty = replace(empty, model_sha256=fitted_identity(empty))
    state = query_joint_price_nodes_v1(fitted=empty, features=rows.iloc[:1], scenario_gap_bps=[0.])
    assert state.status.iloc[0] == "UNKNOWN_MODEL_DISTRIBUTION" and np.isnan(state.expected_net_bps.iloc[0])
    fragmented = deepcopy(fitted)
    fragmented.recipe["intervals_bps"] = ((-100., -50.), (0., 50.))
    fragmented = replace(fragmented, model_sha256=fitted_identity(fragmented))
    result = joint_price_set_5td_v1(fitted=fragmented,
        d_features=rows.loc[0, [*FEATURES, *MINUTE_FEATURES]].to_dict(), reference_cny=10.,
        legal_low_cny=9.9, legal_high_cny=10.05, tick_cny=.01)
    assert result["intervals_cny"] == ((9.9, 9.95), (10., 10.05))
    assert result["unknown_node_count"] == 4 and result["legal_node_count"] == 16
    values = rows.loc[0, [*FEATURES, *MINUTE_FEATURES]].to_dict()
    unknown = joint_price_set_5td_v1(fitted=empty, d_features=values, reference_cny=10.,
                                  legal_low_cny=10., legal_high_cny=10.01, tick_cny=.01)
    assert unknown["status"] == "UNKNOWN_INPUT_OR_SUPPORT" and unknown["unknown_node_count"] == 2


def test_paired_target_contradiction_fails_before_fit_and_schema_identity(joint_fitted_packet):
    fitted, control, rows, config = joint_fitted_packet
    events = []
    changed = rows.copy()
    changed.loc[0, "path_min_ratio"] = 2.
    with pytest.raises(ValueError, match="paired"):
        train_joint_price_5td_v1(rows=changed, frozen_control=control, configuration=config, before_fit=events.append)
    assert events == []
    broken = deepcopy(fitted)
    broken.recipe["candidate_dimensions"] = 1
    with pytest.raises(ValueError, match="identity"):
        query_joint_price_nodes_v1(fitted=broken, features=rows.iloc[:1], scenario_gap_bps=[0.])
