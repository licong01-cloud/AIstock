from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pytest

from backend.services.hmm_risk import formal_state_calendar as subject
from backend.services.hmm_risk import formal_state_model as numeric


def test_fresh_process_uses_task_code_without_database_import():
    root = Path(__file__).resolve().parents[3]
    code = """
import builtins, importlib, json, pathlib, sys
old = builtins.__import__
def guarded(name, *args, **kwargs):
    if name.startswith(('psycopg', 'backend.db.pg_pool')):
        raise RuntimeError('database import poisoned')
    return old(name, *args, **kwargs)
builtins.__import__ = guarded
modules = [importlib.import_module('backend.services.hmm_risk.' + name)
           for name in ('formal_state_calendar', 'formal_state_domains', 'formal_state_input', 'formal_state_executor')]
root = pathlib.Path.cwd().resolve()
assert all(pathlib.Path(module.__file__).resolve().is_relative_to(root) for module in modules)
print(json.dumps({'root': str(root), 'executable': sys.executable, 'imports': len(modules), 'db_poison': 'PASS'}))
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=root,
        env={**os.environ, "PYTHONPATH": str(root)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "root": str(root),
        "executable": sys.executable,
        "imports": 4,
        "db_poison": "PASS",
    }


@pytest.fixture
def source():
    # Local shape fixture, not a production trading-calendar authority.
    dates = [(date(2024, 7, 1) + timedelta(days=i)).isoformat() for i in range(181)] + ["2025-03-31"]
    observations = np.tile([-3.0, -3.0], (182, 1))
    observations[0, 1] = np.nan
    components = {
        name: {"positions": list(range(1, 182)), "values": [0.01] * 181} for name in subject.COMPONENT_WEIGHTS
    }
    components["excess_return_20d"] = {"positions": list(range(2, 182)), "values": [0.02] * 180}
    carrier = subject.build_calendar_carrier(
        dates=dates,
        feature_names=["first", "inactive"],
        observations=observations,
        components=components,
        source_identity_sha256="a" * 64,
        source_receipt_sha256="b" * 64,
    )
    model = numeric.restore_model(
        {
            "startprob": [1 / 3] * 3,
            "transmat": [[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]],
            "means": [[-3, -3], [0, 0], [3, 3]],
            "covariance": [[1, 1]] * 3,
        }
    )
    return model, dict(
        carrier=carrier,
        dates=dates,
        feature_names=["first", "inactive"],
        source_identity_sha256="a" * 64,
        source_receipt_sha256="b" * 64,
        processed_values=np.asarray(carrier["observation_values_f64"]),
        selected_identity={"family": "legacy_covfix", "level": "L1", "sector": "801780.SI", "seed": 42},
    )


def test_raw_full_feature_mask_calendar_ledger_and_zero_refit_readback(source, monkeypatch):
    model, arguments = source
    monkeypatch.setattr(model, "fit", lambda *_: pytest.fail("refit accessed"))
    result = subject.evaluate_calendar_evidence(model, **arguments)
    assert len(result["ledger"]) == len(result["posterior"]) == 182
    assert result["ledger"][0]["missing_feature_names"] == ["inactive"]
    assert result["ledger"][0]["mode"] == "transition_only"
    assert result["ledger"][1]["mode"] == "emission_update"
    assert result["ledger"][1]["evidence_included"] is False
    assert result["evidence_positions"] == list(range(2, 182))
    assert result["diagnostic_tie_positions"] == [0]
    assert 0 not in result["diagnostic_hard_assignment_positions"]
    assert result["assignment_status"] == "accepted"
    assert not {subject.OBSERVATION_UNAVAILABLE, subject.UTILITY_UNAVAILABLE} & set(result["reasons"])
    subject.validate_calendar_evidence(result, model, **arguments)
    result["ledger"][1]["evidence_included"] = True
    result = numeric.receipt({k: v for k, v in result.items() if k != "receipt_sha256"})
    with pytest.raises(numeric.FormalStateError, match="readback"):
        subject.validate_calendar_evidence(result, model, **arguments)


@pytest.mark.parametrize(
    "drift", ["mask", "bool_position", "utility", "source", "missing_names", "version", "placeholder"]
)
def test_rehashed_carrier_drift_is_rejected(source, drift):
    _, arguments = source
    value = copy.deepcopy(arguments["carrier"])
    if drift == "mask":
        value["observation_available_mask"][0] = True
    elif drift == "bool_position":
        value["components"]["excess_return_5d"]["component_available_positions"][0] = True
    elif drift == "utility":
        value["combined_values_f64"][0] = 1.0
    elif drift == "source":
        value["daily_sources"][0]["source_receipt_sha256"] = "c" * 64
        item = value["daily_sources"][0]
        item["receipt_sha256"] = numeric.receipt({k: v for k, v in item.items() if k != "receipt_sha256"})[
            "receipt_sha256"
        ]
    elif drift == "missing_names":
        value["daily_sources"][0]["missing_feature_names"] = []
    elif drift == "version":
        value["schema_version"] = "hmm_risk_d6_validation_calendar_series_v0"
    else:
        value["observation_values_f64"][0][0] = None
    if drift != "placeholder":
        value["manifest"] = subject._manifest({k: v for k, v in value.items() if k != "manifest"})
    with pytest.raises(numeric.FormalStateError) as error:
        subject.validate_calendar_carrier(
            value,
            **{k: v for k, v in arguments.items() if k not in {"carrier", "processed_values", "selected_identity"}},
        )
    assert error.value.reason_code == subject.MISMATCH


def test_finite_compact_empty_sentinel_preserves_all_dates(source):
    model, arguments = source
    components = {name: {"positions": [], "values": []} for name in subject.COMPONENT_WEIGHTS}
    arguments["carrier"] = subject.build_calendar_carrier(
        dates=arguments["dates"],
        feature_names=arguments["feature_names"],
        observations=np.full((182, 2), np.nan),
        components=components,
        source_identity_sha256="a" * 64,
        source_receipt_sha256="b" * 64,
    )
    arguments["processed_values"] = np.empty((0, 2))
    result = subject.evaluate_calendar_evidence(model, **arguments)
    assert result["assignment_status"] == "accepted" and result["evidence_status"] == "failed"
    assert result["evidence_positions"] == [] and len(result["ledger"]) == 182
    assert "hmm_risk_semantic_validation_evidence_rows_insufficient" in result["reasons"]
    assert result["states"] == [] and result["gaps"] == []
    assert all(item["mode"] == "transition_only" for item in result["ledger"])
    subject.validate_calendar_evidence(result, model, **arguments)


def test_assignment_failure_keeps_typed_calendar_ledger(source, monkeypatch):
    model, arguments = source
    monkeypatch.setattr(numeric, "causal_filter", lambda *_: np.tile([0.5, 0.5, 0.0], (182, 1)))
    result = subject.evaluate_calendar_evidence(model, **arguments)
    assert result["assignment_status"] == "failed" and result["evidence_status"] == "insufficient_evidence"
    assert result["reasons"] == ["hmm_risk_semantic_validation_posterior_tie"]
    assert len(result["ledger"]) == 182 and result["input_manifest"] == arguments["carrier"]["manifest"]
    assert len(result["posterior"]) == 182  # Known valid posterior survives the hard-evidence failure.
    assert result["diagnostic_tie_positions"] == list(range(182))
    assert result["diagnostic_hard_assignment_values"] == [] and "evidence_hard_assignments" not in result
    subject.validate_calendar_evidence(result, model, **arguments)
