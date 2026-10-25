"""New matrix/heads and bounded price adapter contracts only."""
from dataclasses import replace
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.generic_moneyflow_price_5td_contracts_v1 import (
    FEATURES, KEY, MATRIX_ORDER, MODEL_FEATURES, MONEYFLOW_FEATURES, PARAMETERS, POLICY, POLICY_SHA256, SCHEMA_SHA256,
)
from backend.services.advisory_model_first.generic_moneyflow_price_5td_models_v1 import (
    GenericMoneyflowPrice5TDFitV1, fitted_identity, matrix_v1, moneyflow_price_set_5td_v1,
    query_moneyflow_nodes_v1, train_moneyflow_price_5td_v1,
)
from backend.services.advisory_model_first.generic_price_5td_models_v1 import (
    GenericPrice5TDFitV1, fitted_identity as control_identity, matrix as control_matrix,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha
from backend.tests.advisory_model_first.test_generic_moneyflow_price_5td_source_v1 import projected_development


def frozen_models(rows, config):
    eligible = rows[KEY[0]].le(pd.Timestamp(config.train_end)) & rows.label_status.eq("AVAILABLE")
    train = rows.loc[eligible].sort_values(list(KEY))
    recipe = dict(features=list(FEATURES), medians=[0.]*9, policy_sha256=POLICY_SHA256, parameters=PARAMETERS,
                  label_contract=POLICY["label_contract"], configuration=config.model_dump(mode="json"))
    models = {f"{arm}_{head}": dict(kind="linear", coef=[0.]*(19 if arm == "candidate" else 18),
              intercept=1.03 if head == "mean" else .96) for arm in ("candidate", "matched") for head in ("mean", "path")}
    control = GenericPrice5TDFitV1(recipe, models, ((-200., -100.), (0., 200.)),
        dict(training_keys_sha256=sha([[str(value) for value in row] for row in train.loc[:, KEY].itertuples(index=False, name=None)])), "")
    control = replace(control, model_sha256=control_identity(control))
    new_recipe = dict(features=list(MODEL_FEATURES), matrix_order=list(MATRIX_ORDER), base_medians=[0.]*9,
        moneyflow_medians=[.2, .3, -.1], schema_sha256=SCHEMA_SHA256, policy_sha256=POLICY_SHA256, parameters=PARAMETERS,
        label_contract=POLICY["label_contract"], configuration=config.model_dump(mode="json"),
        frozen_control_model_sha256=control.model_sha256, frozen_control_arm="candidate_19D")
    new = GenericMoneyflowPrice5TDFitV1(new_recipe,
        {head: dict(kind="gbdt", features=25, initial=1.03 if head == "mean" else .96, learning_rate=1.,
                    trees=[dict(left=[-1], right=[-1], feature=[-2], threshold=[-2.], value=[0.])]) for head in ("mean", "path")},
        control.intervals_bps, {}, "")
    return control, replace(new, model_sha256=fitted_identity(new))


def test_prefix_exact_parity_missing_block_and_funding_changes(tmp_path):
    rows, config, _, _ = projected_development(tmp_path)
    control, fitted = frozen_models(rows, config)
    part = rows.head(3)
    x = matrix_v1(part, base_medians=control.recipe["medians"], moneyflow_medians=[.2, .3, -.1], gaps=[50.]*3)
    assert x.shape == (3, 25) and np.array_equal(x[:, :19], control_matrix(part, control.recipe["medians"], gaps=[50.]*3))
    assert x[0, 22:].tolist() == [1., 1., 1.] and x[1, 22:].tolist() == [0., 0., 0.]
    assert x[0, 19:22].tolist() == [.2, .3, -.1]
    fitted.models["mean"]["trees"] = [dict(left=[1, -1, -1], right=[2, -1, -1], feature=[19, -2, -2],
        threshold=[0., -2., -2.], value=[0., -.08, .02])]
    fitted = replace(fitted, model_sha256=fitted_identity(fitted))
    a = query_moneyflow_nodes_v1(fitted=fitted, features=part, scenario_gap_bps=[50.]*3)
    other = part.copy()
    other.loc[1, MONEYFLOW_FEATURES[0]] = -.8
    other["package_id"] = "OTHER_PACKAGE"
    b = query_moneyflow_nodes_v1(fitted=fitted, features=other, scenario_gap_bps=[50.]*3)
    assert a.loc[1, "expected_net_bps"] != b.loc[1, "expected_net_bps"]
    assert a.loc[2, "expected_net_bps"] == b.loc[2, "expected_net_bps"]
    assert a.loc[2, "input_sha256"] == b.loc[2, "input_sha256"]
    expected = 10000*(1.05*(1-5.95/10000)/(1.005*(1+.95/10000))-1)
    assert a.loc[2, "expected_net_bps"] == pytest.approx(expected)
    assert MONEYFLOW_FEATURES[0] in a.loc[0, "input_unknown_fields"]


def test_only_two_heads_same_keys_and_json_parity(tmp_path):
    rows, config, _, _ = projected_development(tmp_path)
    control, _ = frozen_models(rows, config)
    calls = []
    fitted = train_moneyflow_price_5td_v1(rows=rows, frozen_control=control, configuration=config,
        before_fit=lambda head: calls.append(("start", head)), after_fit=lambda head: calls.append(("end", head)))
    assert calls == [("start", "mean"), ("end", "mean"), ("start", "path"), ("end", "path")]
    assert fitted.diagnostics["physical_fit_count"] == 2 and fitted.diagnostics["train_rows"] == 175
    assert fitted.diagnostics["moneyflow_known_train_counts"][MONEYFLOW_FEATURES[0]] == 174
    assert fitted.diagnostics["training_keys_sha256"] == control.diagnostics["training_keys_sha256"]
    assert all(body["kind"] == "gbdt" and body["features"] == 25 for body in fitted.models.values())
    with pytest.raises(ValueError, match="KEYs differ"):
        train_moneyflow_price_5td_v1(rows=rows.iloc[1:], frozen_control=control, configuration=config,
            before_fit=lambda head: pytest.fail("identity mismatch must have zero fit"), after_fit=lambda head: None)


def test_legal_grid_support_holes_unknown_and_foreign_family(tmp_path):
    rows, config, _, _ = projected_development(tmp_path)
    control, fitted = frozen_models(rows, config)
    fields = rows.iloc[1].loc[list(MODEL_FEATURES)].to_dict()
    kwargs = dict(fitted=fitted, d_features=fields, reference_cny=100, legal_low_cny=98,
                  legal_high_cny=102, tick_cny=Decimal("0.5"))
    result = moneyflow_price_set_5td_v1(**kwargs)
    assert result["intervals_cny"] == ((98., 99.), (100., 102.)) and result["unknown_node_count"] == 1
    assert result["legal_node_count"] == 9 and result["deployable"] is False
    scaled = moneyflow_price_set_5td_v1(**{**kwargs, "reference_cny": 1, "legal_low_cny": .98,
        "legal_high_cny": 1.02, "tick_cny": Decimal("0.005")})
    assert scaled["intervals_cny"] == ((.98, .99), (1., 1.02)) and scaled["legal_node_count"] == 9
    empty = moneyflow_price_set_5td_v1(**{**kwargs, "legal_low_cny": 100.01, "legal_high_cny": 100.02, "tick_cny": 1})
    assert empty["status"] == "EMPTY_LEGAL_GRID"
    unknown = {**fields, **{name: None for name in FEATURES}}
    assert moneyflow_price_set_5td_v1(**{**kwargs, "d_features": unknown})["status"] == "UNKNOWN_INPUT_OR_SUPPORT"
    fitted.models["mean"]["initial"] = .8
    avoid = replace(fitted, model_sha256=fitted_identity(fitted))
    assert moneyflow_price_set_5td_v1(**{**kwargs, "fitted": avoid, "legal_low_cny": 100})["status"] == "NO_ACCEPTABLE_PRICE"
    with pytest.raises(ValueError, match="identity differs"):
        query_moneyflow_nodes_v1(fitted=control, features=rows.head(1), scenario_gap_bps=[50.])
