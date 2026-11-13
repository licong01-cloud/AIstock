"""Admission, time and immutable source tests for the stock risk study."""
from copy import deepcopy
import hashlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from scripts import qe_stock_probability_gate as gate
from backend.services.quantevolver.stock_probability_consumer import (
    apply_stock_probability, bind_buy_candidate_hook, extract_stock_probability,
)


LEGACY = '''class ScoreWeightedTopkStrategyV2:
    def generate_trade_decision(self):
        ranked = scores.sort_values(ascending=False)
        topk_stocks = set(ranked.head(self.topk).index.tolist())
        buy_candidates = [(sid, scores[sid]) for sid in topk_stocks if sid not in current_holdings]
        buy_candidates.sort(key=lambda x: -x[1])
        return buy_candidates
'''


def manifest():
    m = {"schema_version": gate.SCHEMA, "contract": gate.CONTRACT, "arm": "F", "threshold": gate.THRESHOLDS["F"],
         "policy": "block_new_buy_and_add_only", "unknown_policy": "block_buy_keep_base_hold_sell",
         "training_cutoff": "2025-12-31", "dataset_manifest_sha256": gate.DATASET_SHA,
         "risk": {"path": gate.RISK_FILE, "sha256": gate.RISK_HASHES["F"], "rows": 967083},
         "prediction_sha256": sorted(gate.PREDICTION_HASHES)[0],
         "start": "2026-01-05", "end": "2026-09-29", "decision_days": 180,
         "dataset_binding": {"schema_version": "qe_direct_v2_dataset_binding_v3", "release_id": "qe_hmm_full_v2_20260930",
             "cutoff": "2026-09-30", "provider_uri_day": "/frozen/day", "provider_uri_1min": "/frozen/minute",
             "candidate_root": "/frozen", "day_pins": {"calendar_sha256": "day"},
             "minute_pins": {"calendar_sha256": "minute"},
             "selection_pins": {"instrument_name": "stock_universe", "instruments_sha256": "pit"}}}
    return sign(m)


def sign(m):
    m["manifest_sha256"] = gate.canonical_sha({k: v for k, v in m.items() if k != "manifest_sha256"})
    return m


def panel():
    calendar = pd.DatetimeIndex(pd.to_datetime(["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08"]))
    frame = pd.DataFrame([{"signal_date": day, "input_as_of": prior, "instrument": sid,
        "probability": p, "availability": "unknown" if p is None else "available",
        "reason": "TEN_INPUT_MASK" if p is None else None}
        for day, prior in zip(calendar[1:3], calendar[:2])
        for sid, p in [("000001.SZ", .1), ("000002.SZ", None)]])
    return calendar, frame


def test_manifest_rejects_parameter_search_and_wrong_identity():
    gate.validate_manifest(manifest())
    for field, value, code in [("threshold", .2, "THRESHOLD"), ("training_cutoff", "2026-06-01", "POLICY"),
                                ("dataset_manifest_sha256", "changed", "DATASET_IDENTITY"),
                                ("end", "2026-09-30", "ACCOUNT_WINDOW")]:
        m = manifest()
        m[field] = value
        with pytest.raises(gate.StockProbabilityContractError, match=code):
            gate.validate_manifest(sign(m))
    m = manifest()
    m["arm"] = "G"
    with pytest.raises(gate.StockProbabilityContractError, match="MANIFEST_HASH"):
        gate.validate_manifest(m)


@pytest.mark.parametrize("problem,code", [("duplicate", "DUPLICATE"), ("missing", "ROWS_MISSING"),
    ("future", "EXTRA_LAG"), ("nan_available", "AVAILABLE_PROBABILITY"),
    ("safe_unknown", "UNKNOWN_PROBABILITY"), ("no_reason", "UNKNOWN_PROBABILITY")])
def test_risk_panel_never_defaults_missing_to_safe(problem, code):
    cal, frame = panel()
    if problem == "duplicate":
        frame = pd.concat([frame, frame.iloc[:1]], ignore_index=True)
    elif problem == "missing":
        frame = frame.iloc[1:]
    elif problem == "future":
        frame.loc[0, "input_as_of"] = frame.loc[0, "signal_date"]
    elif problem == "nan_available":
        frame.loc[0, "probability"] = np.nan
    elif problem == "safe_unknown":
        frame.loc[1, "probability"] = 0
    else:
        frame.loc[1, "reason"] = None
    with pytest.raises(gate.StockProbabilityContractError, match=code):
        gate.validate_risk_frame(frame, cal)


def test_threshold_equality_unknown_and_previous_session():
    cal, frame = panel()
    obj = gate.StockProbabilityInputs.__new__(gate.StockProbabilityInputs)
    obj.calendar, obj.decision_dates, obj.threshold, obj._day_cache = cal, cal[2:], .1, {}
    obj.frame = gate.validate_risk_frame(frame, cal)
    assert obj.evaluate("SZ000001", "2026-01-07", "2026-01-06")["allowed"]
    assert obj.evaluate("SZ000002", "2026-01-07", "2026-01-06")["action"] == "UNKNOWN_BLOCK"
    obj.threshold = .099
    assert obj.evaluate("SZ000001", "2026-01-07", "2026-01-06")["action"] == "RISK_WARNING_BLOCK"
    with pytest.raises(gate.StockProbabilityContractError, match="DECISION_TIME"):
        obj.evaluate("SZ000001", "2026-01-07", "2026-01-05")
    with pytest.raises(gate.StockProbabilityContractError, match="STOCK_MISSING"):
        obj.evaluate("SZ999999", "2026-01-07", "2026-01-06")


def test_actual_catalog_extension_is_identity_and_idempotent():
    bound = bind_buy_candidate_hook(LEGACY)
    assert bound.count("self._prepare_buy_candidates(") == 1
    assert bind_buy_candidate_hook(bound) == bound
    bad = bound.replace("return buy_candidates\n", "return []\n", 1)
    with pytest.raises(gate.StockProbabilityContractError, match="NOT_IDENTITY"):
        bind_buy_candidate_hook(bad)


def test_compose_only_explicit_gate_preserves_base_contract(tmp_path, monkeypatch):
    # Small isolated transport fixture; full admitted assets are independently validated.
    source = tmp_path / "risk.parquet"
    source.write_bytes(b"transport-fixture")
    monkeypatch.setitem(gate.RISK_HASHES, "F", hashlib.sha256(source.read_bytes()).hexdigest())
    m = manifest()
    conf = {"qlib_init": {"provider_uri": {"day": "/frozen/day", "1min": "/frozen/minute"}}, "market": "stock_universe",
        "port_analysis_config": {"backtest": {"start_time": m["start"], "end_time": m["end"]},
        "strategy": {"class": "SuspendFilterScoreWeightedTopkStrategyV2", "module_path": "qe_suspend_filter_score_weighted_strategy",
                     "kwargs": {"topk": 50, "suspend_filter_strict": True}}}}
    result = {"experiment_files": {"conf.yaml": yaml.safe_dump(conf), "custom_strategy.py": LEGACY},
              "direct_v2_dataset_binding": deepcopy(m["dataset_binding"]), "wsl_command": "python qrun_limit_minute.py conf.yaml"}
    assert apply_stock_probability(result, None, scripts_dir="absent") is result
    outputs = []
    for enabled in (False, True):
        params, study = extract_stock_probability({"qe_stock_probability_enabled": enabled,
            "qe_stock_probability_study": {"manifest": m, "risk_source": str(source)}, "other": 1})
        assert params == {"other": 1}
        out = apply_stock_probability(deepcopy(result), study, scripts_dir=Path(__file__).resolve().parents[3] / "scripts")
        outputs.append(out)
        assert out["wsl_command"].index("qe_stock_probability_gate.py") < out["wsl_command"].index("qrun_limit_minute.py")
        assert yaml.safe_load(out["experiment_files"]["conf.yaml"])["port_analysis_config"]["backtest"] == conf["port_analysis_config"]["backtest"]
    assert outputs[0]["experiment_files"]["conf.yaml"] == result["experiment_files"]["conf.yaml"]
    assert outputs[0]["experiment_files"]["custom_strategy.py"] == outputs[1]["experiment_files"]["custom_strategy.py"]
    result["direct_v2_dataset_binding"]["selection_pins"]["instruments_sha256"] = "wrong"
    with pytest.raises(gate.StockProbabilityContractError, match="BINDING_MISMATCH"):
        apply_stock_probability(result, study, scripts_dir="absent")
