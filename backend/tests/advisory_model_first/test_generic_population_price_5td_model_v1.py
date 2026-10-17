"""P27 kernel contracts with synthetic unit fits, not registered research/profit evidence."""
from copy import deepcopy
from dataclasses import replace
import json

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, KEY, POLICY, POLICY_SHA256, STOCK_FEATURES
from backend.services.advisory_model_first.generic_price_5td_models_v1 import matrix
from backend.services.advisory_model_first.generic_population_price_5td_contracts_v1 import MATRIX_ORDER
from backend.services.advisory_model_first.generic_population_price_5td_model_v1 import (
    fitted_identity_v1, forest_leaf_ids_v1, joint_weights_v1, model_from_payload_v1, model_payload_v1,
    population_price_set_5td_v1, query_population_price_nodes_v1, train_population_price_5td_v1, weighted_quantile_v1,
)


@pytest.fixture(scope="module")
def training():
    records = []
    for role, dates in (("STRUCTURE", pd.bdate_range("2024-07-04", periods=100)),
                        ("ESTIMATION", pd.bdate_range("2024-12-24", periods=100))):
        for i, d in enumerate(dates):
            for extra, symbol in enumerate(("600000.SH", "600001.SH")):
                records.append(dict(zip(KEY, (d, d+pd.offsets.BDay(1), symbol), strict=True)) |
                    dict(zip(FEATURES, (i/1000, .02, .03, .04, .5, 1., .01, .02, np.nan), strict=True)) |
                    dict(observed_gap_bps=20., gross_terminal_ratio=1.02+.02*(i % 2)+extra*.01,
                         path_min_ratio=.99, label_status="AVAILABLE", label_information_end=d+pd.offsets.BDay(5),
                         policy_sha256=POLICY_SHA256, label_contract=POLICY["label_contract"],
                         matched_anchor_pool="NOT_TRAIN_SUPERVISION" if extra else role,
                         candidate_transfer_pool=role))
    frame = pd.DataFrame(records)
    encoding = dict(matrix_order=list(MATRIX_ORDER), medians=[0.]*9,
        intervals_bps=[[-50., -5.], [5., 50.]], estimation_first_D="2024-12-24",
        pools={"matched_anchor": {"STRUCTURE": 100, "ESTIMATION": 100},
               "candidate_transfer": {"STRUCTURE": 200, "ESTIMATION": 200}})
    return frame, encoding


def fit(frame, encoding, arm="matched_anchor", events=None):
    events = [] if events is None else events
    return train_population_price_5td_v1(clusters=frame, encoding=encoding, arm=arm, input_plan_sha256="a"*64,
        before_fit=lambda name: events.append(("before", name)), after_fit=lambda name: events.append(("after", name)))


@pytest.fixture(scope="module")
def fitted(training):
    frame, encoding = training
    return fit(frame, encoding)


def test_fresh_two_arms_same_19d_recipe_unique_mass_and_attempt_hooks(training, fitted):
    frame, encoding = training
    events = []
    candidate = fit(frame, encoding, "candidate_transfer", events)
    assert events == [("before", "candidate_transfer"), ("after", "candidate_transfer")]
    for name in ("parameters", "matrix_order", "medians", "intervals_bps", "estimation_first_D", "encoding_sha256"):
        assert fitted.recipe[name] == candidate.recipe[name]
    assert fitted.recipe["dimensions"] == 19 and len(candidate.forest) == 128
    assert len(fitted.calibration["keys"]) == 100 and len(candidate.calibration["keys"]) == 200
    assert candidate.diagnostics["physical_fit_count"] == 1 and not candidate.diagnostics["deployable"]
    with pytest.raises(ValueError, match="19D"):
        forest_leaf_ids_v1(candidate.forest, np.zeros((1, 39)))


def test_nontraining_poison_is_unread_and_estimation_targets_never_split_trees(training, fitted):
    frame, encoding = training
    poisoned = frame.iloc[[0]].copy()
    poisoned = poisoned.assign(**{n: "never decode evaluation/held labels" for n in
                                  (*FEATURES, "gross_terminal_ratio", "path_min_ratio")})
    poisoned[KEY[0]], poisoned[KEY[1]] = pd.Timestamp("2025-06-03"), pd.Timestamp("2025-06-04")
    poisoned["label_information_end"] = pd.Timestamp("2025-06-10")
    poisoned["matched_anchor_pool"] = "NOT_TRAIN_SUPERVISION"
    unchanged = fit(pd.concat([frame, poisoned], ignore_index=True), encoding)
    assert unchanged.model_sha256 == fitted.model_sha256
    changed = frame.copy()
    changed.loc[changed.matched_anchor_pool.eq("ESTIMATION"), ["gross_terminal_ratio", "path_min_ratio"]] = [2., .8]
    honest = fit(changed, encoding)
    assert honest.forest == fitted.forest and honest.model_sha256 != fitted.model_sha256


def test_nonexecuting_json_roundtrip_and_original_query_unknowns(fitted, training):
    clone = model_from_payload_v1(json.loads(json.dumps(model_payload_v1(fitted))))
    frame = training[0].iloc[:4].loc[:, list(FEATURES)].copy()
    frame.index = [81, 29, 63, 11]
    frame.loc[29, list(STOCK_FEATURES)] = np.nan
    for n in ("selection_effective_rank", "score", "package_id"):
        frame[n] = [1, 2, 3, 4] if n != "package_id" else "original"
    before = query_population_price_nodes_v1(fitted=fitted, features=frame, scenario_gap_bps=[20, 20, 0, None])
    after = query_population_price_nodes_v1(fitted=clone, features=frame, scenario_gap_bps=[20, 20, 0, None])
    pd.testing.assert_frame_equal(before, after)
    assert list(before.index) == [81, 29, 63, 11]
    assert list(before.status) == ["ACCEPTABLE", "UNKNOWN_STOCK_INPUT", "UNKNOWN_GAP_SUPPORT", "UNKNOWN_PRICE_SCENARIO"]
    assert before.loc[81, "distribution_known_trees"] == 128 and "score" not in before
    assert not before.deployable.any() and not before.effective_sample_size_is_time_independence.any()
    weights, _ = joint_weights_v1(forest=fitted.forest, calibration=fitted.calibration,
        x=matrix(frame.iloc[:1], fitted.recipe["medians"], gaps=[20]))
    terminal = np.asarray(fitted.calibration["terminal"])
    net = 10000*(terminal*(1-5.95/10000)/(1.002*(1+.95/10000))-1)
    assert before.loc[81, "expected_net_bps"] == pytest.approx(weights[0] @ net)
    assert before.loc[81, "profit_probability"] == pytest.approx(weights[0] @ (net > 0))
    assert before.loc[81, "downside_q90_bps"] == pytest.approx(10000*(1-.99/1.002))


def test_joint_leaf_weights_same_paired_mass_and_no_global_fallback(fitted, training):
    tree = dict(feature=[0, -2, -2], threshold=[.5, -2., -2.], left=[1, -1, -1], right=[2, -1, -1],
                members={"1": [0, 1], "2": [2]})
    other = deepcopy(tree)
    other["members"] = {"1": [1], "2": [0, 2]}
    empty = deepcopy(tree)
    empty["members"] = {"2": [0, 1, 2]}
    x = np.zeros((2, 19))
    x[1, 0] = 1.
    calibration = dict(terminal=[1., 1.1, 1.2], path=[.9, .95, 1.])
    weights, known = joint_weights_v1(forest=[tree, other, empty], calibration=calibration, x=x)
    np.testing.assert_allclose(weights, [[.25, .75, 0], [5/18, 1/9, 11/18]])
    assert list(known) == [2, 3] and np.allclose(weights.sum(axis=1), 1.)
    none, count = joint_weights_v1(forest=[empty], calibration=calibration, x=x[:1])
    assert not none.any() and count[0] == 0
    # A q10(L) transform is not q90(risk) at an exact 10% atom boundary.
    assert weighted_quantile_v1([.9, 1.], [.1, .9], .1) == .9
    assert weighted_quantile_v1([1000., 0.], [.1, .9], .9) == 0.
    assert weighted_quantile_v1([-9., 1., 99.], [0., 1., 0.], 1.) == 1.
    assert np.isnan(weighted_quantile_v1([1.], [0.], .9))
    blank = deepcopy(fitted)
    no_members = dict(feature=[0, -2, -2], threshold=[1., -2., -2.], left=[1, -1, -1], right=[2, -1, -1],
                      members={"2": list(range(len(blank.calibration["terminal"])))})
    blank = replace(blank, forest=tuple(deepcopy(no_members) for _ in range(128)))
    blank = replace(blank, model_sha256=fitted_identity_v1(blank))
    row = training[0].iloc[:1].loc[:, list(FEATURES)]
    unknown = query_population_price_nodes_v1(fitted=blank, features=row, scenario_gap_bps=[20])
    assert unknown.status.iloc[0] == "UNKNOWN_MODEL_DISTRIBUTION" and unknown.expected_net_bps.isna().all()


def test_complete_decimal_tick_grid_holes_and_empty_sets(fitted, training):
    features = training[0].iloc[0].loc[list(FEATURES)].to_dict()
    result = population_price_set_5td_v1(fitted=fitted, d_features=features,
        reference_cny=100, legal_low_cny=99.5, legal_high_cny=100.5, tick_cny=.01)
    assert result["legal_node_count"] == 101 and result["unknown_node_count"] == 9
    assert result["intervals_cny"] == (("99.50", "99.95"), ("100.05", "100.50"))
    assert result["evidence_use"] == "NAVIGATION_ONLY" and not result["deployable"]
    empty = population_price_set_5td_v1(fitted=fitted, d_features=features,
        reference_cny=100, legal_low_cny=100.001, legal_high_cny=100.009, tick_cny=.01)
    assert empty["status"] == "EMPTY_LEGAL_GRID" and empty["intervals_cny"] == ()
    losing = deepcopy(fitted)
    losing.calibration["terminal"] = [.99]*len(losing.calibration["terminal"])
    losing = replace(losing, model_sha256=fitted_identity_v1(losing))
    avoided = population_price_set_5td_v1(fitted=losing, d_features=features,
        reference_cny=100, legal_low_cny=100.05, legal_high_cny=100.5, tick_cny=.01)
    assert avoided["status"] == "NO_ACCEPTABLE_PRICE" and avoided["intervals_cny"] == ()


@pytest.mark.parametrize("fault", ["policy", "duplicate_mass", "tree_cycle"])
def test_serialized_semantic_faults_fail_even_with_recomputed_checksum(fitted, fault):
    broken = deepcopy(fitted)
    if fault == "policy":
        broken.recipe["policy_sha256"] = "b"*64
    elif fault == "duplicate_mass":
        members = broken.forest[0]["members"]
        members[next(iter(members))].append(0)
    else:
        broken.forest[0]["left"][0] = 0
    broken = replace(broken, model_sha256=fitted_identity_v1(broken))
    with pytest.raises(ValueError, match="policy|mass|cycle"):
        model_payload_v1(broken)


def test_label_overlap_rejected_before_physical_fit(training):
    frame, encoding = training
    broken = frame.copy()
    broken.loc[0, "label_information_end"] = pd.Timestamp("2024-12-24")
    events = []
    with pytest.raises(ValueError, match="label end"):
        fit(broken, encoding, events=events)
    assert events == []
