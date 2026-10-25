"""Honest D distributions, non-executable serialization and monotone value price sets."""
from copy import deepcopy
from dataclasses import asdict, replace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.d_clock_population_price_5td_contracts_v1 import ARMS, FEATURES
from backend.services.advisory_model_first.d_clock_population_price_5td_inputs_v1 import build_training_encoding_v1
from backend.services.advisory_model_first.d_clock_population_price_5td_model_v1 import (
    d_clock_model_from_payload_v1, d_clock_model_payload_v1, d_clock_population_price_set_v1,
    freeze_d_clock_distribution_v1, query_frozen_d_clock_nodes_v1, train_d_clock_population_model_v1,
)
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha
from backend.tests.advisory_model_first.test_d_clock_population_price_5td_inputs_v1 import synthetic_training_rows


@pytest.fixture(scope="module")
def fitted():
    rows, plan = synthetic_training_rows()
    encoding = build_training_encoding_v1(rows=rows, plan=plan)
    calls = []
    result = train_d_clock_population_model_v1(rows=rows, encoding=encoding, arm=ARMS[0], plan_sha256="a"*64,
        before_fit=lambda a: calls.append(("before", a)), after_fit=lambda a: calls.append(("after", a)))
    assert calls == [("before", ARMS[0]), ("after", ARMS[0])]
    return result


def test_serialized_d_distribution_is_price_invariant_with_exact_single_fees(fitted):
    model = d_clock_model_from_payload_v1(d_clock_model_payload_v1(fitted))
    d = pd.DataFrame([{n: .1 for n in FEATURES}]*3)
    frozen = freeze_d_clock_distribution_v1(fitted=model, d_features=d)
    assert frozen.leaf_ids.iloc[0] == frozen.leaf_ids.iloc[2]
    q = query_frozen_d_clock_nodes_v1(fitted=model, frozen=frozen, price_cny=[9., 10., 11.], reference_cny=[10.]*3)
    assert np.diff(q.expected_net_bps).max() < 0
    assert np.diff(q.downside_q90_bps).min() >= 0
    # The same distribution and one buy/sell charge imply this payment identity.
    assert np.allclose((q.expected_net_bps/10000+1)*np.array([9., 10., 11.]), (q.expected_net_bps.iloc[1]/10000+1)*10.)
    assert q.probability_clock.eq("D_HYPOTHETICAL_PAYMENT_NOT_ACTUAL_OPEN_CONDITIONAL").all()


def test_full_decimal_grid_unknown_and_empty_are_distinct(fitted):
    args = dict(fitted=fitted, d_features={n: .1 for n in FEATURES}, reference_cny=10.,
        legal_low_cny=8.005, legal_high_cny=13.005, tick_cny=.01)
    result = d_clock_population_price_set_v1(**args)
    assert result["complete_nodes"] == 500 and result["nodes"].price_cny.iloc[0] == "8.01"
    assert result["bands_cny"][0]["low"] == "8.01"
    args["d_features"] = {n: None for n in FEATURES}
    result = d_clock_population_price_set_v1(**args)
    assert result["status"] == "UNKNOWN" and result["unknown_nodes"] == 500
    args["d_features"], args["legal_low_cny"], args["legal_high_cny"] = {n: .1 for n in FEATURES}, 100., 101.
    assert d_clock_population_price_set_v1(**args)["status"] == "EMPTY_ACCEPTABLE_SET"


def test_held_poison_cannot_change_fit_and_tampered_mass_is_rejected(fitted):
    rows, plan = synthetic_training_rows()
    encoding = build_training_encoding_v1(rows=rows, plan=plan)
    extra = rows.iloc[:1].copy()
    extra["pool_"+ARMS[0]] = "NOT_TRAIN_SUPERVISION"
    extra[list(FEATURES)] = "opaque held feature"
    extra["terminal_mark_ratio"] = "opaque held outcome"
    replay = train_d_clock_population_model_v1(rows=pd.concat([rows, extra], ignore_index=True), encoding=encoding,
        arm=ARMS[0], plan_sha256="a"*64, before_fit=lambda a: None, after_fit=lambda a: None)
    assert replay.model_sha256 == fitted.model_sha256
    broken = deepcopy(fitted.calibration)
    broken["masses"][0] = 2.
    with pytest.raises(ValueError, match="identity"):
        d_clock_model_payload_v1(replace(fitted, calibration=broken))


def test_empty_query_leaf_has_no_global_fallback_and_arrow_roundtrip_preserves_distribution(fitted, tmp_path):
    n = len(fitted.calibration["terminal"])
    tree = dict(feature=[0, -2, -2], threshold=[.5, -2., -2.], left=[1, -1, -1], right=[2, -1, -1], members={"1": list(range(n))})
    body = asdict(fitted)
    body["forest"] = [deepcopy(tree) for _ in range(128)]
    body["model_sha256"] = sha({k: v for k, v in body.items() if k != "model_sha256"})
    model = d_clock_model_from_payload_v1(body)
    frozen = freeze_d_clock_distribution_v1(fitted=model, d_features=pd.DataFrame([{k: .9 for k in FEATURES}]))
    p = tmp_path/"frozen.parquet"
    p.write_bytes(parquet_bytes(frozen))
    q = query_frozen_d_clock_nodes_v1(fitted=model, frozen=pd.read_parquet(p), price_cny=[10.], reference_cny=[10.])
    assert q.iloc[0].status == "UNKNOWN_MODEL_DISTRIBUTION" and pd.isna(q.iloc[0].expected_net_bps)
