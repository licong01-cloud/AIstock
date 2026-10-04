from __future__ import annotations

import copy
import math
import subprocess
import sys

import pandas as pd
import pytest
from sklearn.exceptions import ConvergenceWarning

from backend.services.hmm_risk import risk_l2 as risk
from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES
from backend.services.hmm_risk.formal_state_model import FormalStateError, receipt


def _calendar():
    # Synthetic count/boundary fixture; the real builder separately pins day.txt.
    def days(start, end):
        return [d.date().isoformat() for d in pd.bdate_range(start, end)]

    train = days(risk.TRAIN_START, "2024-06-14")
    dev = days(risk.DEV_START, "2026-03-17")
    return (
        days("2020-07-30", "2022-01-03")
        + [train[0]]
        + train[-590:]
        + days("2024-06-17", risk.TRAIN_END)
        + [dev[0]]
        + dev[-413:]
        + days("2026-03-18", risk.DEV_END)
    )


def _panel(monkeypatch):
    codes = [f"801{i:03d}.SI" for i in range(131)]
    train, dev = ["2024-01-02", "2024-01-03"], ["2024-07-01", "2024-07-02"]
    as_of = {d: d for d in train + dev}
    plan = {"train": train, "fit": train, "dev": dev, "mature": dev, "as_of": as_of}
    monkeypatch.setattr(risk, "schedule", lambda _calendar: plan)
    rows = {}
    targets = {}
    for day in train + dev:
        rows[day] = {
            c: {"as_of_date": day, "features": [float(i % 7)] + [0.0] * 19, "reason_code": None}
            for i, c in enumerate(codes)
        }
        if day in train:
            targets[day] = {
                c: {
                    "status": "AVAILABLE",
                    "event": int(i % 7 >= 3),
                    "drawdown": -0.1 if i % 7 >= 3 else 0.0,
                    "return": -0.1 if i % 7 >= 3 else 0.02,
                }
                for i, c in enumerate(codes)
            }
    return receipt(
        {
            "schema_version": risk.VERSION + "_features",
            "contract": risk.CONTRACT,
            "feature_names": list(ALL_CORE_FEATURES),
            "calendar": train + dev,
            "catalog": codes,
            "rows": rows,
            "train_labels": targets,
            "input_identity": {"fixture": True},
        }
    )


def _reseal(payload):
    return receipt({k: v for k, v in payload.items() if k != "receipt_sha256"})


def test_calendar_purge_asof_and_warmup_are_exact():
    plan = risk.schedule(_calendar())
    assert [len(plan[k]) for k in ("train", "fit", "dev", "mature")] == [601, 591, 424, 414]
    assert plan["as_of"][risk.DEV_START] == "2024-06-28"
    with pytest.raises(FormalStateError):
        risk.schedule([d for d in _calendar() if d != "2026-03-17"])


def test_absolute_drawdown_uses_t_plus_one_peak_and_legal_na():
    days = [d.date().isoformat() for d in pd.bdate_range("2024-01-01", periods=12)]
    returns = {d: {"x": 0.0} for d in days}
    returns[days[0]]["x"] = -0.9
    returns[days[1]]["x"] = 0.1
    returns[days[2]]["x"] = -0.081
    y = risk.drawdown_outcomes(days, [days[0], days[-1]], ["x"], returns, days[-1])
    assert y[days[0]]["x"]["drawdown"] == pytest.approx(-0.081)
    assert y[days[0]]["x"]["event"] == 1
    assert y[days[-1]]["x"]["status"] == "OUTCOME_NOT_MATURE"
    returns[days[3]]["x"] = None
    assert (
        risk.drawdown_outcomes(days, [days[0]], ["x"], returns, days[-1])[days[0]]["x"]["status"] == "OUTCOME_LEGAL_NA"
    )
    returns[days[4]]["x"] = math.nan
    with pytest.raises(FormalStateError, match="unknown"):
        risk.drawdown_outcomes(days, [days[0]], ["x"], returns, days[-1])


def test_pooled_train_only_scaler_weights_class1_and_inactive_future(monkeypatch):
    panel = _panel(monkeypatch)
    first = risk.fit_predict(panel)
    changed = copy.deepcopy(panel)
    changed["rows"]["2024-07-01"][panel["catalog"][0]]["features"][19] = 1234
    second = risk.fit_predict(_reseal(changed))
    assert first["fits"] == second["fits"] == 1
    assert first["parameters"] == second["parameters"]
    assert first["predictions"] == second["predictions"]
    assert second["inactive_changed_rows"] == 1
    assert first["sample_weight_sum"] == pytest.approx(first["fit_rows"])
    assert len(first["parameters"]["coef"][0]) == 1
    assert all(0 <= r["probability"] <= 1 and r["warning"] == (r["probability"] >= 0.2) for r in first["predictions"])


@pytest.mark.parametrize("fault", ["asof", "feature", "label", "catalog", "contract"])
def test_feature_errors_fail_closed(monkeypatch, fault):
    panel = copy.deepcopy(_panel(monkeypatch))
    row = panel["rows"]["2024-07-01"][panel["catalog"][0]]
    if fault == "asof":
        row["as_of_date"] = "2099-01-01"
    elif fault == "feature":
        row["features"].pop()
    elif fault == "label":
        panel["train_labels"]["2024-01-02"][panel["catalog"][0]]["event"] = True
    elif fault == "catalog":
        panel["catalog"].pop()
    else:
        panel["contract"] = {**risk.CONTRACT, "warning_threshold": 0.1}
    with pytest.raises(FormalStateError):
        risk.fit_predict(_reseal(panel))


def test_one_class_is_evidence_insufficient_not_constant_model(monkeypatch):
    panel = copy.deepcopy(_panel(monkeypatch))
    for labels in panel["train_labels"].values():
        for row in labels.values():
            row.update({"event": 0, "drawdown": 0.0, "return": 0.02})
    sealed = risk.fit_predict(_reseal(panel))
    assert sealed["fits"] == 0 and sealed["status"] == "TRAIN_LABEL_EVIDENCE_INSUFFICIENT"
    assert len(sealed["predictions"]) == 262
    assert risk.evaluate(sealed, {}, panel["calendar"])["effect_status"] == "EVIDENCE_INSUFFICIENT"


def test_nonconverged_model_is_failed_without_retry(monkeypatch):
    panel = _panel(monkeypatch)
    calls = []

    def reject(*_a, **_k):
        calls.append(1)
        raise ConvergenceWarning("test")

    monkeypatch.setattr(risk.LogisticRegression, "fit", reject)
    with pytest.raises(FormalStateError, match="no retry"):
        risk.fit_predict(panel)
    assert calls == [1]


def test_metric_empty_denominators_hac_and_no_warning_are_honest():
    counts = risk._counts([{"probability": 0.01, "event": 1, "warning": False, "drawdown": -0.1, "return": -0.1}])
    assert counts["precision"] is None and counts["recall"] == 0 and counts["FN"] == 1
    assert counts["missed_drawdown_mean"] == -0.1
    assert risk._hac([], [], counts)["status"] == "HAC_UNAVAILABLE"
    assert risk._counts([])["base_rate"] is None


def test_evaluation_keeps_past_population_baseline_ties_and_all_legal_labels(monkeypatch):
    panel = _panel(monkeypatch)
    sealed = risk.fit_predict(panel)
    labels = {
        d: {
            c: {
                "status": "AVAILABLE",
                "event": 1 if i % 7 >= 3 else 0,
                "drawdown": -0.1 if i % 7 >= 3 else 0.0,
                "return": -0.1,
            }
            for i, c in enumerate(panel["catalog"])
        }
        for d in ("2024-07-01", "2024-07-02")
    }
    result = risk.evaluate(sealed, labels, panel["calendar"])
    assert result["metrics"]["overall"]["M"] == 262
    assert all(r["baseline_warning_count"] == 0 for r in result["metrics"]["daily"])
    assert result["metrics"]["overall"]["precision_lift"] > 0
    assert len(result["predictions"]) == 262
    labels["2024-07-01"][panel["catalog"][0]].update(
        {"status": "OUTCOME_LEGAL_NA", "event": None, "drawdown": None, "return": None}
    )
    assert risk.evaluate(sealed, labels, panel["calendar"])["metrics"]["overall"]["M"] == 261
    unavailable = copy.deepcopy(sealed)
    unavailable["predictions"][1].update(
        probability=None,
        warning=None,
        availability="unavailable",
        structural_eligible=False,
        reason_code="hmm_risk_c010_observation_unavailable",
        volatility_Nd=None,
    )
    result = risk.evaluate(_reseal(unavailable), labels, panel["calendar"])
    assert result["predictions"][1]["event"] == labels["2024-07-01"][panel["catalog"][1]]["event"]
    assert result["metrics"]["overall"]["M"] == 260


def test_fresh_process_import_does_not_touch_database():
    code = """import builtins
old = builtins.__import__
def guarded(name, *args, **kwargs):
    if name.startswith(("psycopg", "backend.db", "backend.data_service")):
        raise AssertionError(name)
    return old(name, *args, **kwargs)
builtins.__import__ = guarded
from backend.services.hmm_risk import risk_l2
assert risk_l2.PARAMS["random_state"] == 42
"""
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr


def test_child_seals_before_development_labels_and_parent_rejects_identity_drift(tmp_path, monkeypatch):
    from scripts.hmm_risk import run_risk_l2 as cli
    from backend.services.hmm_risk.formal_state_executor import read_json, write_once

    panel = _panel(monkeypatch)
    labels = {
        d: {
            c: {
                "status": "AVAILABLE",
                "event": int(i % 7 >= 3),
                "drawdown": -0.1 if i % 7 >= 3 else 0.0,
                "return": -0.1,
            }
            for i, c in enumerate(panel["catalog"])
        }
        for d in ("2024-07-01", "2024-07-02")
    }
    facts = receipt(
        {
            "schema_version": risk.VERSION + "_outcome_facts",
            "feature_sha256": panel["receipt_sha256"],
            "calendar": panel["calendar"],
            "catalog": panel["catalog"],
            "tail_accessed": False,
            "returns": {},
        }
    )
    feature_file, fact_file, output = (tmp_path / n for n in ("features.json", "facts.json", "repeat.json"))
    write_once(feature_file, panel)
    write_once(fact_file, facts)
    events = []
    original_read, original_write = cli.read_json, cli.write_once

    def read(path):
        if path == fact_file:
            events.append("target_access")
        return original_read(path)

    def write(path, body):
        original_write(path, body)
        if path.suffixes[-2:] == [".sealed", ".json"]:
            events.append("sealed")

    monkeypatch.setattr(cli, "read_json", read)
    monkeypatch.setattr(cli, "write_once", write)
    monkeypatch.setattr(cli.subprocess, "check_output", lambda *_a, **_k: "a" * 40)
    monkeypatch.setattr(
        cli, "numeric_environment", lambda: {"versions": {}, "thread_variables": {}, "thread_pools": []}
    )
    monkeypatch.setattr(risk, "drawdown_outcomes", lambda *_a, **_k: labels)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_risk_l2",
            "child",
            "--request",
            str(feature_file),
            "--outcome-facts",
            str(fact_file),
            "--output",
            str(output),
            "--request-sha256",
            panel["receipt_sha256"],
            "--outcome-sha256",
            facts["receipt_sha256"],
            "--executor-commit",
            "a" * 40,
        ],
    )
    assert cli.main() == 0
    assert events == ["sealed", "target_access"]
    repeat = read_json(output)
    binding = dict(
        feature_sha256=panel["receipt_sha256"], outcome_sha256=facts["receipt_sha256"], executor_commit="a" * 40
    )
    assert risk.close_processes(repeat, copy.deepcopy(repeat), **binding)["completed_fits"] == 2
    bad = _reseal({**repeat, "outcome_sha256": "b" * 64})
    with pytest.raises(FormalStateError, match="parent"):
        risk.close_processes(bad, copy.deepcopy(bad), **binding)


def test_source_file_drift_fails_before_any_file_builder(tmp_path):
    path = tmp_path / "source.json"
    path.write_text("{}", encoding="utf-8")
    with pytest.raises(FormalStateError, match="frozen request"):
        risk.prepare_file_inputs(path, work_parent=tmp_path / "work", source_commit="a" * 40)


def test_unknown_and_purged_labels_are_not_silently_skipped(monkeypatch):
    panel = copy.deepcopy(_panel(monkeypatch))
    target = panel["train_labels"]["2024-01-02"][panel["catalog"][0]]
    target["status"] = "SKIPPED"
    with pytest.raises(FormalStateError, match="maturity"):
        risk.fit_predict(_reseal(panel))
    with pytest.raises(FormalStateError, match="maturity"):
        risk._validate_target({"status": "AVAILABLE", "event": 0, "drawdown": 0.0, "return": 0.0}, mature=False)
