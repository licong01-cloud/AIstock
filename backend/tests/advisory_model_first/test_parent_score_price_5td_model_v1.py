"""Synthetic model parity and economic price contracts; not real research evidence."""
from dataclasses import replace
import json

import numpy as np
import pytest

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import ARMS
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import training_encoding
from backend.services.advisory_model_first.parent_score_price_5td_model_v1 import (
    _matrix, fit_identity, forest_leaf_ids, joint_cluster_weights, model_from_payload,
    model_payload, parent_score_price_set, query_parent_score_nodes, train_parent_score_model,
)
from backend.tests.advisory_model_first.test_parent_score_price_5td_inputs_v1 import sample_plan, sample_rows


@pytest.fixture(scope="module")
def fitted_pair(tmp_path_factory):
    plan, calendar = sample_plan(tmp_path_factory.mktemp("parent_model_fixture"))
    rows, _ = sample_rows(plan, calendar)
    encoding = training_encoding(rows=rows, plan=plan)
    events = []
    fitted = {arm: train_parent_score_model(rows=rows, encoding=encoding, arm=arm, plan_sha256=plan.plan_sha256,
        before_fit=lambda a: events.append(("before", a)), after_fit=lambda a: events.append(("after", a))) for arm in ARMS}
    assert len(events) == 4
    return fitted, rows, encoding


def test_two_dimensions_json_leaf_parity_and_cluster_mass(fitted_pair):
    fits, rows, encoding = fitted_pair
    for arm, fitted in fits.items():
        restored = model_from_payload(json.loads(json.dumps(model_payload(fitted))))
        assert restored.model_sha256 == fitted.model_sha256 and restored.recipe["dimensions"] == (20 if arm == ARMS[0] else 22)
        query = rows.loc[rows.pool.eq("ESTIMATION")].iloc[:3]
        x, known, _ = _matrix(query, encoding, arm, np.zeros(3))
        assert known.all()
        w, trees, _, _ = joint_cluster_weights(fitted=restored, x=x)
        assert np.allclose(w.sum(axis=1), 1) and (trees == 128).all()
        assert w.shape[1] == len(set(fitted.calibration["clusters"]))
    tree = dict(feature=[21, -2, -2], threshold=[1., -2., -2.], left=[1, -1, -1], right=[2, -1, -1], members={})
    x = np.zeros((2, 22))
    x[:, 21] = [1.+1e-10, 1.001]
    assert forest_leaf_ids(forest=[tree], x=x, dimensions=22).tolist() == [[1], [2]]
    with pytest.raises(ValueError, match="20/22D"):
        forest_leaf_ids(forest=[tree], x=np.zeros((1, 19)), dimensions=19)


def test_missing_score_adapter_and_support_are_unknown_not_matched_fallback(fitted_pair):
    fits, rows, _ = fitted_pair
    query = rows.iloc[:3].copy()
    query.loc[query.index[0], "parent_score"] = np.nan
    query.loc[query.index[1], "manifest_sha256"] = "f"*64
    result = query_parent_score_nodes(fitted=fits[ARMS[1]], features=query, scenario_gap_bps=[0., 0., 900.])
    assert result.status.tolist() == ["UNKNOWN_PARENT_SCORE", "UNKNOWN_PACKAGE_ADAPTER", "UNKNOWN_GAP_SUPPORT"]
    matched = query_parent_score_nodes(fitted=fits[ARMS[0]], features=query.iloc[:1], scenario_gap_bps=[0.])
    assert matched.status.iloc[0] in ("ACCEPTABLE", "AVOID")


def test_direct_risk_cdf_atom_decimal_complete_grid_and_no_mass(fitted_pair):
    fits, rows, _ = fitted_pair
    original = fits[ARMS[1]]
    calibration = {n: values[:10] for n, values in original.calibration.items()}
    calibration.update(terminal=[1.03]*10, path=[.98]*9+[.75], masses=[1.]*10)
    # Ten distinct stock/D clusters: the original fixture's first ten are distinct.
    recipe = json.loads(json.dumps(original.recipe))
    recipe["encoding"]["pools"]["ESTIMATION"] = 10
    from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha
    recipe["encoding_sha256"] = sha(recipe["encoding"])
    diagnostics = dict(original.diagnostics, estimation_rows=10, estimation_keys_sha256=sha(calibration["keys"]))
    stump = dict(feature=[-2], threshold=[-2.], left=[-1], right=[-1], members={"0": list(range(10))})
    model = replace(original, recipe=recipe, forest=tuple(stump for _ in range(128)), calibration=calibration, diagnostics=diagnostics)
    model = replace(model, model_sha256=fit_identity(model))
    query = rows.iloc[:1]
    node = query_parent_score_nodes(fitted=model, features=query, scenario_gap_bps=[0.])
    assert node.status.iloc[0] == "ACCEPTABLE" and node.downside_q90_bps.iloc[0] == pytest.approx(200.)
    context = {n: query.iloc[0][n] for n in ("package_id", "manifest_sha256", "run_id", "parent_score")}
    kwargs = dict(fitted=model, d_features={n: query.iloc[0][n] for n in FEATURES}, source_context=context,
                  reference_cny="10", legal_low_cny="9.90", legal_high_cny="10.10", tick_cny="0.01")
    prices = parent_score_price_set(**kwargs)
    assert prices["legal_node_count"] == 21 and prices["unknown_node_count"] > 0
    assert prices["unknown_intervals_cny"] and all(v["reason"] == "UNKNOWN_GAP_SUPPORT" for v in prices["unknown_intervals_cny"])
    assert prices["actual_fill_proven"] is False and prices["price_basis"] == "D_ANCHORED_CNY"
    empty = parent_score_price_set(**dict(kwargs, legal_low_cny="10.001", legal_high_cny="10.009"))
    assert empty["status"] == "EMPTY_LEGAL_GRID"
    no_members = dict(feature=[0, -2, -2], threshold=[100., -2., -2.], left=[1, -1, -1], right=[2, -1, -1], members={"2": list(range(10))})
    no_mass = replace(model, forest=tuple(no_members for _ in range(128)))
    no_mass = replace(no_mass, model_sha256=fit_identity(no_mass))
    assert query_parent_score_nodes(fitted=no_mass, features=query, scenario_gap_bps=[0.]).status.iloc[0] == "UNKNOWN_MODEL_DISTRIBUTION"
