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


def _semantic_path(hard, *, positions=None, contract=numeric.L2_SEMANTIC_VERSION):
    # Shape-only calendar fixture, never a production trading-calendar authority.
    dates = [(date(2024, 7, 1) + timedelta(days=i)).isoformat() for i in range(181)] + ["2025-03-31"]
    posterior = np.eye(3)[hard]
    components = {
        name: {"positions": list(range(182)), "values": (np.asarray(hard) * 0.01).tolist()}
        for name in subject.COMPONENT_WEIGHTS
    }
    arguments = dict(dates=dates, positions=list(range(182)) if positions is None else positions, components=components)
    return numeric.semantic_posterior_evidence(posterior, **arguments, contract_version=contract), posterior, arguments


def test_l2_single_long_runs_keep_common_gates_actual_transitions_and_old_contract():
    hard = [0] * 60 + [1] * 60 + [2] * 62
    revised, _, _ = _semantic_path(hard)
    old, _, _ = _semantic_path(hard, contract=numeric.CONTRACTS["semantic"])
    assert revised["semantic_evidence_valid"] and not old["semantic_evidence_valid"]
    assert revised["mapping"] == {"0": "fading", "1": "neutral", "2": "trending"}
    assert [(s["incoming"], s["outgoing"]) for s in revised["states"]] == [(0, 1), (1, 1), (1, 0)]
    assert all(s["structural_path"] == "persistent" for s in revised["states"])
    assert revised["states"][0]["left_censored"] and revised["states"][2]["right_censored"]
    assert all("structural_path" not in s for s in old["states"])
    for key in ("count", "occupancy", "months", "incoming", "outgoing", "utility_mean", "utility_variance"):
        assert [s[key] for s in old["states"]] == [s[key] for s in revised["states"]]


def test_old_calendar_rejection_still_precedes_causal_filter(monkeypatch):
    monkeypatch.setattr(numeric, "causal_filter", lambda *_: pytest.fail("invalid calendar reached filtering"))
    with pytest.raises(numeric.FormalStateError, match="validation calendar"):
        numeric.semantic_evidence(None, dates=[], positions=[], values=[], components={})


@pytest.mark.parametrize("first,second,path", [(18, 2, "recurrent"), (19, 1, "persistent")])
def test_l2_share_boundary_and_no_operator_path_choice(first, second, path):
    hard = [1] * 30 + [0] * first + [2] * 40 + [0] * second
    hard += [1] * (182 - len(hard))
    result, _, _ = _semantic_path(hard)
    assert result["states"][0]["structural_path"] == path
    assert result["states"][0]["runs"] == 2


def test_l2_internal_na_cannot_bridge_runs_or_claim_full_window_censor():
    hard = [0] * 60 + [1] * 60 + [2] * 62
    result, _, _ = _semantic_path(hard, positions=[p for p in range(182) if p not in {0, 30, 181}])
    first, _, last = result["states"]
    assert first["runs"] == 2 and first["max_run_share"] < 0.9
    assert not first["left_censored"] and not last["right_censored"]
    assert (first["incoming"], first["outgoing"]) == (0, 1)
    assert first["incoming_required"] == 2
    assert not result["semantic_evidence_valid"] and result["mapping"] is None


@pytest.mark.parametrize("count", [1, 2, 3])
def test_l2_persistent_never_repairs_singleton_or_rare_state(count):
    result, _, _ = _semantic_path([0] * count + [1] * 90 + [2] * (92 - count))
    assert result["states"][0]["structural_path"] is None
    assert not result["semantic_evidence_valid"] and result["mapping"] is None
    assert "hmm_risk_semantic_validation_state_count_insufficient" in result["reasons"]


def test_l2_tie_unknown_contract_and_reinterpretation_identity_are_fail_closed(source, monkeypatch):
    hard = [0] * 60 + [1] * 60 + [2] * 62
    _, posterior, arguments = _semantic_path(hard)
    posterior[60] = [0.5, 0.5, 0.0]
    with pytest.raises(numeric.FormalStateError, match="tie"):
        numeric.semantic_posterior_evidence(posterior, **arguments, contract_version=numeric.L2_SEMANTIC_VERSION)
    with pytest.raises(numeric.FormalStateError, match="unknown semantic"):
        numeric.semantic_posterior_evidence(posterior, **arguments, contract_version="unknown")
    model, original_arguments = source
    monkeypatch.setattr(model, "fit", lambda *_: pytest.fail("fit accessed"))
    value = subject.evaluate_calendar_evidence(model, **original_arguments)
    reinterpret_arguments = {k: v for k, v in original_arguments.items() if k != "processed_values"}
    with pytest.raises(numeric.FormalStateError, match="identity"):
        subject.reinterpret_l2_evidence(value, **reinterpret_arguments)


def test_l2_pinned_evidence_reinterpretation_preserves_hashes_and_detects_rehashed_drift(source, monkeypatch):
    model, arguments = source
    hard = [0] * 60 + [1] * 60 + [2] * 62
    posterior = np.eye(3)[hard]
    arguments["selected_identity"] = {"family": "autocycle_all_core", "level": "L2", "sector": "801783.SI", "seed": 47}
    monkeypatch.setattr(numeric, "causal_filter", lambda *_: posterior)
    monkeypatch.setattr(model, "fit", lambda *_: pytest.fail("fit accessed"))
    value = subject.evaluate_calendar_evidence(model, **arguments)
    reinterpret_arguments = {k: v for k, v in arguments.items() if k != "processed_values"}
    result = subject.reinterpret_l2_evidence(value, **reinterpret_arguments)
    assert result["selected_model_parameter_sha256"] == value["selected_model_parameter_sha256"]
    assert result["original_semantic_receipt_sha256"] == value["receipt_sha256"]
    # Fixture utility is deliberately tied: structural repair must not fabricate mapping.
    assert result["mapping"] is None
    value["ledger"][10]["source_receipt_sha256"] = "c" * 64
    value = numeric.receipt({k: v for k, v in value.items() if k != "receipt_sha256"})
    with pytest.raises(numeric.FormalStateError, match="identity"):
        subject.reinterpret_l2_evidence(value, **reinterpret_arguments)


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
