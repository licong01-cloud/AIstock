"""Synthetic direct matrix, not a formal new-window run or model qualification."""

from copy import deepcopy
import json
import subprocess
import sys

import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import LogisticRegression

from backend.services.hmm_risk import frozen_l2_history as h
from backend.services.hmm_risk import risk_l2 as risk
from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, canonical_sha256
from backend.services.hmm_risk.formal_state_model import receipt
from backend.tests.hmm_risk.test_rotation_l2_moneyflow_supervised import _calendar


@pytest.fixture
def rotation(monkeypatch):
    closed = {"2026-04-06", "2026-05-01", "2026-05-04", "2026-05-05", "2026-06-19"}
    calendar = [d.isoformat() for d in _calendar()] + [
        d.date().isoformat() for d in pd.bdate_range("2026-04-01", "2026-08-31") if d.date().isoformat() not in closed
    ]
    codes = [f"801{i:03d}.SI" for i in range(131)]
    daily = [
        {
            "trade_date": day,
            "sector_code": c,
            "structural_eligible": True,
            "eligible": True,
            "reason_code": None,
            "expected_contributors": 2,
            "valid_contributors": 2,
            "coverage": 1.0,
            "net_mf_amount_cny": (i + 1) * (j + 1),
            "amount_cny": 1000.0,
            "maximum_member_amount_share": 0.6,
        }
        for j, day in enumerate(calendar[:-1])
        for i, c in enumerate(codes)
    ]
    view = {
        "sector_returns": [
            {"trade_date": day, "sector_code": c, "quote_available": True, "pct_change": (i - 65) / 100}
            for day in calendar[:-1]
            for i, c in enumerate(codes)
        ],
        "benchmark_close": [{"trade_date": day, "close": 1000.0 + i} for i, day in enumerate(calendar[:-1])],
    }
    first = calendar.index("2026-04-01")
    feature_days = set(calendar[first - 25 : -1])
    daily = [r for r in daily if r["trade_date"] in feature_days]
    view["sector_returns"] = [r for r in view["sector_returns"] if r["trade_date"] in feature_days]
    view["benchmark_close"] = [r for r in view["benchmark_close"] if r["trade_date"] in set(calendar[first - 26 : -1])]
    parameters = {}
    for arm, api in (("rank", h.price), ("return", h.value.return_model)):
        parameters[arm] = h.ridge.seal(
            {
                "contract_hash": api.MODEL_CONTRACT_HASH,
                "feature_names": list(api.FEATURE_NAMES),
                "coefficients": [0.2, 0.3, 0.1, -0.1],
                "intercept": 0.02,
            },
            "parameter_sha256",
        )
        pins = dict(h.value.RANK_PINS if arm == "rank" else h.value.RETURN_PINS)
        pins["model_parameter_sha256"] = parameters[arm]["parameter_sha256"]
        monkeypatch.setattr(h.value, "RANK_PINS" if arm == "rank" else "RETURN_PINS", pins)
    bundle = receipt(
        {
            "schema_version": h.VERSION + "_rotation_features",
            "contract": h.CONTRACT,
            "calendar": calendar,
            "catalog": codes,
            "daily_aggregates": daily,
            "price_features": view,
        }
    )
    return bundle, parameters


def reseal(payload):
    return receipt({k: v for k, v in payload.items() if k != "receipt_sha256"})


def test_exact_history_ledger_and_original_guards(rotation):
    bundle, _ = rotation
    plan = h.schedule(bundle["calendar"])
    assert (len(plan["days"]), len(plan["mature"]), plan["mature"][-1]) == (104, 94, "2026-08-17")
    with pytest.raises(Exception, match="tail date forbidden"):
        h.value.replay_path(plan["days"], plan["mature"], {d: [] for d in plan["mature"]}, {}, cost_bps=0)
    assert (
        h.ridge.predictions_from_parameters([{"trade_date": "2026-04-01"}], {"coefficients": [], "intercept": 0}) == []
    )


def test_complete_rotation_inference_is_causal_and_zero_fit(rotation):
    bundle, parameters = rotation
    with h.no_training_or_external_actions():
        first = h.infer_rotation(bundle, parameters)
        changed = deepcopy(bundle)
        for r in changed["daily_aggregates"]:
            if r["trade_date"] >= "2026-04-01":
                r["net_mf_amount_cny"] *= -7
        for r in changed["price_features"]["sector_returns"]:
            if r["trade_date"] >= "2026-04-01":
                r["pct_change"] *= -5
        second = h.infer_rotation(reseal(changed), parameters)
    assert first["fits"] == 0
    for arm in ("rank", "return", "delta"):
        assert len(first["predictions"][arm]) == 104 * 131
        assert [r for r in first["predictions"][arm] if r["trade_date"] == "2026-04-01"] == [
            r for r in second["predictions"][arm] if r["trade_date"] == "2026-04-01"
        ]
    bad = deepcopy(parameters)
    bad["rank"]["coefficients"][0] += 1
    with pytest.raises(Exception):
        h.infer_rotation(bundle, bad)


def test_cohort_na_is_not_filled_or_future_filtered():
    days = [d.date().isoformat() for d in pd.bdate_range("2026-04-01", periods=12)]
    groups = {d: ["801019.SI"] for d in days[:2]}
    quotes = {(d, "801019.SI"): 0.01 for d in days}
    quotes[days[5], "801019.SI"] = None
    path = h.value.cohort_reference_path(days, days[:2], groups, quotes, cost_bps=20)
    assert path[0]["entry_count"] == 1 and path[0]["cost"] > 0
    assert all(r["nav"] is None for r in path[5:])
    assert h.value._summary(path, initial_nav=1.0)["status"] == "INSUFFICIENT_REFERENCE_PATH"
    blocks = h._paired_blocks(days, path, path)
    assert blocks["paired_dates"] == 5 and blocks["blocks"][0]["hac"]["status"] == "NOT_COMPUTABLE"


@pytest.mark.parametrize("fault", ["missing", "duplicate", "nonfinite", "future"])
def test_rotation_feature_view_fails_closed(rotation, fault):
    bundle, parameters = rotation
    view = bundle["price_features"]["sector_returns"]
    if fault == "missing":
        view.pop()
    elif fault == "duplicate":
        view[-1] = dict(view[0])
    elif fault == "nonfinite":
        view[0]["pct_change"] = float("inf")
        with pytest.raises(ValueError):
            reseal(bundle)
        return
    else:
        view[-1]["trade_date"] = "2026-08-31"
    with pytest.raises(Exception, match="price feature grid"):
        h.infer_rotation(reseal(bundle), parameters)


def risk_parameters():
    return {
        "mean": [0.0] * 20,
        "scale": [1.0] * 19 + [0.0],
        "active": [True] * 19 + [False],
        "coef": [[0.1] * 19],
        "intercept": [-1.0],
        "classes": [0, 1],
        "iterations": [8],
    }


def test_restored_logistic_uses_exact_original_float64_path_without_fit():
    parameters = risk_parameters()
    value = [float(i) / 20 for i in range(20)]
    rows = {"2026-04-01": {"801783.SI": {"as_of_date": "2026-03-31", "features": value, "reason_code": None}}}
    model = LogisticRegression(**risk.PARAMS)
    model.coef_ = np.asarray(parameters["coef"], dtype=np.float64)
    model.intercept_ = np.asarray(parameters["intercept"], dtype=np.float64)
    model.classes_ = np.asarray([0, 1])
    model.n_features_in_ = 19
    with h.no_training_or_external_actions():
        expected = float(model.predict_proba(np.asarray(value[:19], dtype=np.float64).reshape(1, -1))[0, 1])
        actual, changed = risk.predictions_from_parameters(rows, list(rows), ["801783.SI"], parameters)
    assert actual[0]["probability"] == expected and actual[0]["warning"] == (expected >= 0.2) and changed == 1
    parameters["scale"][0] = 0
    with pytest.raises(Exception, match="scaler shape"):
        risk.predictions_from_parameters(rows, list(rows), ["801783.SI"], parameters)


def test_full_risk_history_keeps_unavailable_budget_and_event_diagnostics(rotation, monkeypatch):
    template, _ = rotation
    parameters = risk_parameters()
    monkeypatch.setitem(h.risk_value.APPROVED_PINS, "model_hash", canonical_sha256(parameters))
    days = h.schedule(template["calendar"])["days"]
    rows = {
        d: {
            c: {
                "as_of_date": h.schedule(template["calendar"])["as_of"][d],
                "features": [0.0] * 20 if i else None,
                "reason_code": None if i else "legal_unavailable",
            }
            for i, c in enumerate(template["catalog"])
        }
        for d in days
    }
    bundle = receipt(
        {
            "schema_version": h.VERSION + "_risk_features",
            "contract": h.CONTRACT,
            "calendar": template["calendar"],
            "catalog": template["catalog"],
            "feature_names": list(ALL_CORE_FEATURES),
            "rows": rows,
        }
    )
    returns = {d: {c: 0.001 for c in bundle["catalog"]} for d in days}
    facts = receipt(
        {
            "schema_version": h.VERSION + "_outcomes",
            "contract": h.CONTRACT,
            "feature_sha256": bundle["receipt_sha256"],
            "event_returns": returns,
            "returns": {d: returns[d] for d in days[1:]},
        }
    )
    with h.no_training_or_external_actions():
        sealed = h.infer_risk(bundle, parameters)
        result = h.evaluate_risk(bundle, sealed, facts)
    assert len(sealed["predictions"]) == 104 * 131 and sealed["fits"] == 0
    reference = result["reference_value"]
    assert reference["planned_return_dates"] == 103 and reference["tail_accessed"] is True
    assert reference["daily"][0]["input_unavailable_cash_count"] == 1
    assert reference["daily"][0]["arms"]["R"]["risk_budget"] == reference["daily"][0]["arms"]["X"]["risk_budget"]
    assert result["mature_days"] == 94 and result["event_diagnostic"]["M"] == 94 * 130


def test_two_fresh_inference_processes_match_without_database():
    code = """import json,sys
from unittest.mock import patch
from backend.services.hmm_risk.risk_l2 import predictions_from_parameters
p=json.load(sys.stdin)
with patch('sklearn.linear_model.LogisticRegression.fit',side_effect=AssertionError('fit')),patch('backend.db.pg_pool.get_conn',side_effect=AssertionError('database')):
 print(json.dumps(predictions_from_parameters({'2026-04-01':{'801783.SI':{'as_of_date':'2026-03-31','features':[0.0]*20,'reason_code':None}}},['2026-04-01'],['801783.SI'],p),sort_keys=True))
"""
    results = [
        subprocess.run(
            [sys.executable, "-c", code],
            input=json.dumps(risk_parameters()),
            text=True,
            capture_output=True,
            check=True,
        ).stdout
        for _ in (1, 2)
    ]
    assert results[0] == results[1]
