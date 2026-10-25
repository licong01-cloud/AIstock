"""Recovery must reject evidence drift and preserve old execution results."""

from copy import deepcopy
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest
import yaml

SCRIPT = Path(__file__).resolve().parents[3] / "scripts/qe_l2_registration_recovery.py"
spec = importlib.util.spec_from_file_location("l2_recovery", SCRIPT)
recovery = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recovery)


def fixture(tmp_path, identifier="6ed6c1be"):
    task, status = recovery.ROSTER[identifier]
    params = {"random_seed": 42, "qe_l2_study_manifest": {"contract": recovery.CONTRACT}}
    conf = {
        "port_analysis_config": {
            "strategy": {"kwargs": {"suspend_filter_strict": True}},
            "executor": {
                "kwargs": {
                    "inner_strategy": {
                        "class": "TailTWAPWithLimitStrategy",
                        "module_path": "tail_twap_strategy",
                        "kwargs": {"unfilled_handler": "TAIL_BOOST"},
                    }
                }
            },
            "backtest": {
                "start_time": "2026-04-01",
                "end_time": "2026-09-30",
                "account": 100_000_000,
                "exchange_kwargs": {"freq": "1min", "limit_threshold": ["$limit_up", "$limit_down"]},
            },
        }
    }
    wire = yaml.safe_dump(conf).encode()
    (tmp_path / "conf.yaml").write_bytes(wire.replace(b"\n", b"\r\n"))
    original = {"task_id": task, "experiment_id": identifier, "custom_params": params}
    preflight = {
        "status": "PASS",
        "plan_sha256": recovery.digest(original),
        "file_hashes": {"conf.yaml": hashlib.sha256(wire).hexdigest()},
    }
    preserved = {
        "task_id": task,
        "status": "PRESERVED_AND_RETROSPECTIVE_MINUTE_GUARDS_PASS",
        "original_worker_packages_unchanged": True,
        "files": {"conf.yaml": {"sha256": hashlib.sha256(wire).hexdigest()}},
    }
    for name, value in [
        ("study_plan.json", original),
        ("remote_preflight.json", preflight),
        ("preservation_receipt.json", preserved),
    ]:
        (tmp_path / name).write_text(json.dumps(value), encoding="utf-8")
    return {
        "experiment_id": identifier,
        "status": status,
        "alpha_mode": "single",
        "custom_params": params,
        "qe_task_id": task if status == "completed" else None,
        "qe_loop_id": "Loop1" if status == "completed" else None,
        "result_metrics": {"final_nav": 0.98} if status == "completed" else None,
    }, conf


def test_frozen_wire_bytes_and_archival_projection(tmp_path):
    from backend.services.qe_archive.source_assembler import QEArchiveSourceAssembler

    row, _ = fixture(tmp_path)
    before = deepcopy(row)
    plan = recovery.build_plan(row, [], tmp_path)
    assert row == before
    assert plan["audit"]["evidence"]["conf_sha256"] != plan["audit"]["evidence"]["local_conf_file_sha256"]
    payload = QEArchiveSourceAssembler.build_experiment_payload(dict(row, custom_params=plan["after_params"]))
    assert payload["freq"] == "1min" and payload["limit_suspend_authoritative"] is True
    assert plan["after_params"]["random_seed"] == 42
    assert not plan["dispatch_authorized"]


@pytest.mark.parametrize("mutation", ["config", "params", "preservation", "status", "reservation"])
def test_changed_source_is_rejected(tmp_path, mutation):
    row, _ = fixture(tmp_path)
    reservations = []
    if mutation == "config":
        with (tmp_path / "conf.yaml").open("ab") as file:
            file.write(b"\n# changed\n")
    elif mutation == "params":
        row["custom_params"]["random_seed"] = 2026
    elif mutation == "preservation":
        value = json.loads((tmp_path / "preservation_receipt.json").read_text())
        value["original_worker_packages_unchanged"] = False
        (tmp_path / "preservation_receipt.json").write_text(json.dumps(value))
    elif mutation == "status":
        row["status"] = "running"
    else:
        reservations = [{"status": "running"}]
    with pytest.raises(ValueError, match="QE_L2_RECOVERY_"):
        recovery.build_plan(row, reservations, tmp_path)


def test_created_requires_no_historical_attempt(tmp_path):
    row, _ = fixture(tmp_path, "d1ab9d9e")
    assert recovery.build_plan(row, [], tmp_path)["created_not_dispatched_proof"]
    with pytest.raises(ValueError, match="CREATED_ALREADY_ATTEMPTED"):
        recovery.build_plan(row, [{"status": "released"}], tmp_path)


@pytest.mark.parametrize("key,value", [("backtest_freq", "day"), ("execution_algo", "VWAP")])
def test_existing_runtime_disagreement_not_overwritten(tmp_path, key, value):
    _, conf = fixture(tmp_path)
    with pytest.raises(ValueError):
        recovery.corrected_params({key: value}, conf)


def test_apply_rejects_non_runtime_mutation_before_write(tmp_path, monkeypatch):
    row, _ = fixture(tmp_path)
    plan = recovery.build_plan(row, [], tmp_path)
    monkeypatch.setattr(recovery, "fetch", lambda *args, **kwargs: (deepcopy(row), []))
    plan["after_params"]["random_seed"] = 2026
    plan["after_params"][recovery.AUDIT_KEY]["corrected_params_sha256"] = recovery.digest(
        {k: v for k, v in plan["after_params"].items() if k != recovery.AUDIT_KEY}
    )
    with pytest.raises(ValueError, match="PLAN_OVERWRITE"):
        recovery.apply_plan(None, plan)


def test_apply_rejects_concurrent_result_change(tmp_path, monkeypatch):
    row, _ = fixture(tmp_path)
    plan = recovery.build_plan(row, [], tmp_path)
    changed = deepcopy(row)
    changed["result_metrics"]["final_nav"] = 1.2
    monkeypatch.setattr(recovery, "fetch", lambda *args, **kwargs: (changed, []))
    with pytest.raises(ValueError, match="CONCURRENT_SOURCE_CHANGE"):
        recovery.apply_plan(None, plan)
