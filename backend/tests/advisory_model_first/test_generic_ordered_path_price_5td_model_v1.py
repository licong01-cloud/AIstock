"""One shared real unit fit; new coordinates and frozen honest roles, not research trials."""
from copy import deepcopy
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.generic_ordered_path_price_5td_contracts_v1 import KEY, ORDERED_FEATURES, RAW_FEATURES, POLICY
from backend.services.advisory_model_first.generic_ordered_path_price_5td_model_v1 import (
    fitted_identity, joint_distribution_v1, matrix_v1, ordered_path_price_set_5td_v1,
    query_ordered_path_nodes_v1, train_ordered_path_price_5td_v1,
)
from backend.tests.advisory_model_first.test_generic_joint_distribution_price_5td_model_v1 import joint_fitted_packet as joint_fitted_packet
from backend.tests.advisory_model_first.test_generic_volume_path_price_5td_model_v1 import volume_fitted_packet as volume_fitted_packet


@pytest.fixture(scope="session")
def ordered_fitted_packet(joint_fitted_packet):
    control, _, source, config = joint_fitted_packet
    rows = source.copy()
    for i, name in enumerate(ORDERED_FEATURES):
        rows[name] = .01 if i % 2 == 0 else 1/16
    rows.loc[0, ORDERED_FEATURES[0]] = np.nan
    events = []
    fitted = train_ordered_path_price_5td_v1(rows=rows, frozen_control=control, configuration=config, before_fit=events.append)
    assert events == ["candidate_ordered_path_forest"]
    return fitted, control, rows, config


def test_same_roles_structure_medians_new_dimensions_cost_and_package_independence(ordered_fitted_packet):
    fitted, control, rows, _ = ordered_fitted_packet
    assert fitted.recipe["medians"][:19] == control.recipe["medians"]
    assert len(fitted.recipe["medians"]) == 51 and fitted.recipe["medians"][19] == .01
    for key in ("structure_keys_sha256", "estimation_keys_sha256"):
        assert fitted.diagnostics[key] == control.diagnostics[key]
    x = matrix_v1(rows.iloc[:1], fitted.recipe["medians"], gaps=[0.], arm="candidate")
    assert x.shape == (1, 103) and x[0, 70] == 1.  # 51 masks plus ordered field index19.
    result = query_ordered_path_nodes_v1(fitted=fitted, features=rows.iloc[:1], scenario_gap_bps=[0.])
    _, mean, lower, _ = joint_distribution_v1(forest=fitted.forest, calibration=fitted.calibration, matrix=x)
    assert result.expected_net_bps.iloc[0] == pytest.approx(10000*(mean[0]*(1-POLICY["sell_bps"]/10000)/(1+POLICY["buy_bps"]/10000)-1))
    assert result.downside_q90_bps.iloc[0] == pytest.approx(10000*max(0., 1-lower[0]))
    for pool in ("stock_universe", "single_index", "index_union"):
        changed = query_ordered_path_nodes_v1(fitted=fitted, features=rows.iloc[:1].assign(package_id="OTHER", universe_identity=pool, combined_score=999.), scenario_gap_bps=[0.])
        pd.testing.assert_frame_equal(result.iloc[:, :6], changed.iloc[:, :6])


def test_future_poison_cannot_affect_model_and_role_drift_fails_before_fit(ordered_fitted_packet):
    fitted, control, rows, config = ordered_fitted_packet
    poison = rows.iloc[:1].copy()
    poison[KEY[0]], poison[KEY[1]] = pd.Timestamp("2024-04-01"), pd.Timestamp("2024-04-02")
    poison.loc[:, list(RAW_FEATURES)] = np.inf
    poison["label_information_end"] = "invalid outside train clock"
    another = train_ordered_path_price_5td_v1(rows=pd.concat([rows, poison]), frozen_control=control, configuration=config, before_fit=lambda _: None)
    assert another.model_sha256 == fitted.model_sha256
    changed = rows.copy()
    changed.loc[0, "label_status"] = "UNKNOWN"
    events = []
    with pytest.raises(ValueError, match="population|roles"):
        train_ordered_path_price_5td_v1(rows=changed, frozen_control=control, configuration=config, before_fit=events.append)
    assert not events


def test_joint_weights_zero_mass_and_full_price_tick_holes(ordered_fitted_packet):
    fitted, _, rows, _ = ordered_fitted_packet
    split = dict(feature=[0, -2, -2], threshold=[0., -2., -2.], left=[1, -1, -1], right=[2, -1, -1], members={"1": [0, 1], "2": [1, 2]})
    flat = dict(feature=[-2], threshold=[-2.], left=[-1], right=[-1], members={"0": [1, 2]})
    calibration = dict(terminal=[.95, 1.05, 1.15], path=[.9, .98, 1.01])
    x = np.zeros((1, 103))
    x[:, 0] = -1.
    weights, mean, lower, _ = joint_distribution_v1(forest=(split, flat), calibration=calibration, matrix=x)
    np.testing.assert_allclose(weights, [[.25, .5, .25]])
    assert mean[0] == pytest.approx(1.05) and lower[0] == .9
    empty = deepcopy(fitted)
    for tree in empty.forest:
        tree["members"] = {}
    empty = replace(empty, model_sha256=fitted_identity(empty))
    state = query_ordered_path_nodes_v1(fitted=empty, features=rows.iloc[:1], scenario_gap_bps=[0.])
    assert state.status.iloc[0] == "UNKNOWN_MODEL_DISTRIBUTION" and np.isnan(state.expected_net_bps.iloc[0])
    holes = deepcopy(fitted)
    holes.recipe["intervals_bps"] = ((-100., -50.), (0., 50.))
    holes = replace(holes, model_sha256=fitted_identity(holes))
    result = ordered_path_price_set_5td_v1(fitted=holes, d_features=rows.loc[0, list(RAW_FEATURES)].to_dict(), reference_cny=10., legal_low_cny=9.9, legal_high_cny=10.05, tick_cny=.01)
    assert result["intervals_cny"] == ((9.9, 9.95), (10., 10.05)) and result["unknown_node_count"] == 4
