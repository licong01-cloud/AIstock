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

from backend.services.hmm_risk import formal_state_executor as executor
from backend.services.hmm_risk import formal_state_model as subject
from backend.services.hmm_risk.contracts import canonical_sha256


@pytest.fixture(scope="module")
def fitted():
    rng = np.random.RandomState(42)
    means = np.tile(np.repeat([-3.0, 0.0, 3.0], 30), 6)
    values = np.column_stack((means + rng.normal(0, 0.4, len(means)), means * 0.7 + rng.normal(0, 0.3, len(means))))
    dates = [(date(2022, 1, 3) + timedelta(days=i)).isoformat() for i in range(len(values))]
    entry = subject.fit_entry(values, dates, 42)
    return values, dates, entry


def test_map_joint_stop_and_zero_refit_readback(fitted):
    values, dates, entry = fitted
    subject.validate_fit_entry(entry, values, dates)
    history = entry["history"]
    assert 2 <= len(history) <= 300
    assert abs(history[-1]["delta"]) <= history[-1]["tolerance"]
    assert entry["covariance"]["covariance_valid"]
    assert entry["final_train_likelihood"] == history[-1]["raw_likelihood"]
    repeated = subject.fit_entry(values, dates, 42)
    assert repeated == entry


@pytest.mark.parametrize("field", ["accepted", "final_train_likelihood", "train_posterior", "structure"])
def test_rehashed_candidate_cannot_forge_acceptance(fitted, field):
    values, dates, original = fitted
    entry = copy.deepcopy(original)
    if field == "accepted":
        entry[field] = not entry[field]
    elif field == "final_train_likelihood":
        entry[field] += 1
    elif field == "train_posterior":
        entry[field][0] = [1.0, 0.0, 0.0]
    else:
        entry[field]["states"][0]["count"] += 1
        entry[field] = subject.receipt({k: v for k, v in entry[field].items() if k != "receipt_sha256"})
    entry = subject.receipt({k: v for k, v in entry.items() if k != "receipt_sha256"})
    with pytest.raises(subject.FormalStateError, match="authority differs"):
        subject.validate_fit_entry(entry, values, dates)


def test_d3_sector_reference_prior_no_projection(fitted):
    values, _, _ = fitted
    model, initialization = subject.initialize(values, 42)
    reference = values.var(axis=0, ddof=0)
    n = np.asarray(initialization["counts"])[:, None]
    s = np.asarray(initialization["cluster_variance"])
    np.testing.assert_array_equal(model._covars_, (n * s + reference) / (n + 1))
    np.testing.assert_array_equal(model.covars_prior, np.tile(reference, (3, 1)))
    assert model.covars_weight == 2 and model.min_covar == 0 and model.init_params == ""


@pytest.mark.parametrize("bad", [0.0, float("nan"), float("inf"), -1.0])
def test_covariance_invalid_never_projected(fitted, bad):
    values, _, entry = fitted
    model = subject.restore_model(entry["model"])
    model._covars_[0, 0] = bad
    with pytest.raises(subject.FormalStateError):
        subject.covariance_audit(model, values, values.var(axis=0))
    assert model._covars_[0, 0] == bad or np.isnan(model._covars_[0, 0])


def test_preprocess_is_train_global_and_immutable():
    x = np.arange(240, dtype=float).reshape(120, 2)
    parameters = subject.preprocess_fit([x, x + 7], subject.FAMILIES[1])
    before = canonical_sha256(parameters)
    assert subject.preprocess_apply(np.array([[1e10, -1e10]]), parameters).shape == (1, 2)
    assert canonical_sha256(parameters) == before
    with pytest.raises(subject.FormalStateError):
        subject.preprocess_fit([np.ones((120, 2))], subject.FAMILIES[1])


def test_train_persistent_path_and_singleton_fail():
    hard = np.tile(np.repeat([0, 1, 2], 60), 3)
    posterior = np.eye(3)[hard]
    dates = [(date(2022, 1, 1) + timedelta(days=i)).isoformat() for i in range(len(hard))]
    accepted = subject.train_structure(posterior, dates)
    assert accepted["train_occupancy_valid"]
    hard[:] = 0
    hard[50] = 1
    hard[80] = 2
    failed = subject.train_structure(np.eye(3)[hard], dates)
    assert not failed["train_occupancy_valid"]


def test_calendar_gaps_break_runs_and_transitions():
    posterior = np.eye(3)[[0, 1, 1, 0]]
    states = subject.hard_structure(posterior, ["2024-07-01"] * 4, [0, 2, 3])
    assert states[1]["incoming"] == 0
    assert states[1]["outgoing"] == 1
    assert states[0]["runs"] == 2


def test_transition_only_first_day_and_gap(fitted):
    values, _, entry = fitted
    model = subject.restore_model(entry["model"])
    posterior = subject.causal_filter(model, [2], values[:1], 4)
    np.testing.assert_allclose(posterior[0], model.startprob_, atol=1e-15)
    np.testing.assert_allclose(posterior[1], posterior[0] @ model.transmat_, atol=1e-15)
    np.testing.assert_allclose(posterior[3], posterior[2] @ model.transmat_, atol=1e-15)
    with pytest.raises(subject.FormalStateError):
        subject.causal_filter(model, [2, 2], values[:2], 4)


def test_hard_ties_are_not_broken_by_state_index():
    with pytest.raises(subject.FormalStateError, match="tie"):
        subject.validate_posterior(np.array([[0.5, 0.5, 0.0]]), 1, require_margin=True)


def _selection_candidates():
    return [
        {
            "seed": seed,
            "entries": {
                "a": {"accepted": True, "final_train_likelihood": float(seed), "training_rows": 1, "feature_count": 1}
            },
        }
        for seed in subject.SEEDS
    ]


def test_d5_lex_pool_schedule_and_validation_invisibility():
    candidates = _selection_candidates()
    selection = subject.select_restart(candidates, ["a"])
    assert selection["selected_seed"] == 49
    assert selection["validation_accessed"] is False
    for candidate in candidates:
        candidate["entries"]["a"]["final_train_likelihood"] = 1.0
    assert subject.select_restart(candidates, ["a"])["selected_seed"] == 42
    with pytest.raises(subject.FormalStateError, match="schedule"):
        subject.select_restart(candidates[:-1], ["a"])
    for candidate in candidates:
        candidate["entries"]["a"]["accepted"] = False
    assert subject.select_restart(candidates, ["a"])["selected_seed"] is None


def test_fresh_process_mismatch_fails_before_d5(monkeypatch):
    request = {"receipt_sha256": "a" * 64}
    first = subject.receipt(
        {
            "request_sha256": "a" * 64,
            "fit_attempts": 2592,
            "selection_performed": False,
            "validation_accessed": False,
            "groups": {},
        }
    )
    second = subject.receipt({**{k: v for k, v in first.items() if k != "receipt_sha256"}, "changed": True})
    monkeypatch.setattr(executor, "select_restart", lambda *_: pytest.fail("D5 accessed"))
    with pytest.raises(subject.FormalStateError, match="bitwise"):
        executor.finalize(request, first, second)


def test_output_no_overwrite_or_fake_readback(tmp_path):
    path = tmp_path / "receipt.json"
    executor.write_once(path, subject.receipt({"ready": False}))
    with pytest.raises(FileExistsError):
        executor.write_once(path, subject.receipt({"ready": True}))
    assert executor.read_json(path)["ready"] is False


def test_numeric_environment_never_changes_dependency_versions(monkeypatch):
    monkeypatch.setenv("OMP_NUM_THREADS", "2")
    with pytest.raises(subject.FormalStateError, match="version/thread"):
        executor.numeric_environment()


def test_persistent_state_requires_both_common_and_persistent_conditions():
    hard = np.zeros(601, dtype=int)
    hard[330:] = np.tile(np.repeat([1, 2], 15), 10)[:271]
    hard[[401, 500]] = 0
    dates = [(date(2022, 1, 1) + timedelta(days=i)).isoformat() for i in range(601)]
    result = subject.train_structure(np.eye(3)[hard], dates)
    assert result["train_occupancy_valid"]
    state = result["states"][0]
    assert state["evidence_path"] == "persistent"
    assert all(state["common_comparisons"].values()) and all(state["path_comparisons"].values())
    hard[401] = hard[400]
    # Still two runs, but only one entering transition: common gate must win.
    result = subject.train_structure(np.eye(3)[hard], dates)
    assert not result["train_occupancy_valid"]
    assert result["states"][0]["evidence_path"] == "none"


def _validation_dates():
    # Calendar validator checks immutable endpoints/length; this local unit
    # fixture is not a production source/calendar authority.
    return [(date(2024, 7, 1) + timedelta(days=i)).isoformat() for i in range(181)] + ["2025-03-31"]


def test_d6_zero_observations_retains_calendar_and_fails_evidence(fitted):
    model = subject.restore_model(fitted[2]["model"])
    components = {f"excess_return_{h}d": {"positions": [], "values": []} for h in (5, 10, 20)}
    result = subject.semantic_evidence(model, dates=_validation_dates(), positions=[], values=[], components=components)
    assert len(result["posterior"]) == 182
    assert result["evidence_positions"] == [] and result["evidence_status"] == "failed"
    assert result["assignment_status"] == "accepted" and result["mapping"] is None


def test_d6_utility_missing_is_not_assignment_failure(fitted):
    model = subject.restore_model(fitted[2]["model"])
    components = {f"excess_return_{h}d": {"positions": [], "values": []} for h in (5, 10, 20)}
    result = subject.semantic_evidence(
        model, dates=_validation_dates(), positions=[0], values=fitted[0][:1], components=components
    )
    assert result["assignment_status"] == "accepted" and result["evidence_status"] == "failed"
    components["excess_return_5d"] = {"positions": [0], "values": [float("nan")]}
    with pytest.raises(subject.FormalStateError) as error:
        subject.semantic_evidence(
            model, dates=_validation_dates(), positions=[0], values=fitted[0][:1], components=components
        )
    assert error.value.reason_code == "hmm_risk_semantic_utility_non_finite"
    assert error.value.evidence["assignment_status"] == "accepted"


def test_d6_diagnostic_tie_outside_evidence_does_not_add_gate(fitted, monkeypatch):
    model = subject.restore_model(fitted[2]["model"])
    posterior = np.tile([0.5, 0.5, 0.0], (182, 1))
    monkeypatch.setattr(subject, "causal_filter", lambda *_: posterior)
    components = {f"excess_return_{h}d": {"positions": [], "values": []} for h in (5, 10, 20)}
    result = subject.semantic_evidence(
        model, dates=_validation_dates(), positions=[0], values=fitted[0][:1], components=components
    )
    assert result["assignment_status"] == "accepted"
    for component in components.values():
        component.update(positions=[0], values=[0.01])
    with pytest.raises(subject.FormalStateError, match="tie"):
        subject.semantic_evidence(
            model, dates=_validation_dates(), positions=[0], values=fitted[0][:1], components=components
        )


def test_signed_zero_repeat_mismatch_is_not_object_equality(monkeypatch):
    request = {"receipt_sha256": "a" * 64}
    body = {
        "request_sha256": "a" * 64,
        "fit_attempts": 2592,
        "selection_performed": False,
        "validation_accessed": False,
        "groups": {},
        "sentinel": 0.0,
    }
    first, second = subject.receipt(body), subject.receipt({**body, "sentinel": -0.0})
    monkeypatch.setattr(executor, "select_restart", lambda *_: pytest.fail("D5 accessed"))
    with pytest.raises(subject.FormalStateError, match="bitwise"):
        executor.finalize(request, first, second)


def test_cli_preflight_missing_request_durable_failure_zero_fit(tmp_path):
    root = Path(__file__).resolve().parents[3]
    output = tmp_path / "preflight.json"
    result = subprocess.run(
        [
            sys.executable,
            str(root / "scripts/hmm_risk/run_formal_state_model_set.py"),
            "preflight",
            "--request",
            str(tmp_path / "missing.json"),
            "--output",
            str(output),
        ],
        env={**os.environ, "PYTHONPATH": str(root)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1 and not output.exists()
    failure = json.loads(output.with_name("preflight.json.failure.json").read_text(encoding="utf-8"))
    assert failure["ready"] is False and failure["database_write"] is False
    assert failure["exception_type"] == "FileNotFoundError"
