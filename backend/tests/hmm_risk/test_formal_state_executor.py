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


@pytest.mark.parametrize("bad", ([[True, False]], [[True, 2]], [["1", "2"]], [[None, 1]], [[1 + 2j, 1]]))
def test_numerical_contract_rejects_coercible_non_numeric_payloads(bad):
    with pytest.raises(subject.FormalStateError, match="real numeric"):
        subject.array(bad, (1, 2), "request values")


def test_preprocess_is_train_global_and_immutable():
    x = np.arange(240, dtype=float).reshape(120, 2)
    parameters = subject.preprocess_fit([x, x + 7], subject.FAMILIES[1])
    before = canonical_sha256(parameters)
    assert subject.preprocess_apply(np.array([[1e10, -1e10]]), parameters).shape == (1, 2)
    assert canonical_sha256(parameters) == before
    with pytest.raises(subject.FormalStateError):
        subject.preprocess_fit([np.ones((120, 2))], subject.FAMILIES[1])


def _projection_fixture():
    rng = np.random.RandomState(42)
    raw = rng.normal(size=(120, 20))
    raw[:, 19] = -0.0
    other_sector = rng.normal(loc=-3.0, size=(800, 20))
    parameters = subject.preprocess_fit([raw, other_sector], subject.FAMILIES[1])
    return raw, parameters


def test_d1_fixed_projection_after_full_preprocess_and_d6_zero_refit():
    raw, parameters = _projection_fixture()
    projected, projection = subject.project_training(
        raw, parameters, family=subject.FAMILIES[1], level="L2", sector="801207.SI", source_receipt_sha256="a" * 64
    )
    full = subject.preprocess_apply(raw, parameters)
    assert np.all(full[:, 19] == full[0, 19]) and full[0, 19] != 0
    assert projected.shape == (120, 19)
    assert projection["feature_count"] == 20 and projection["likelihood_feature_count"] == 19
    assert projection["inactive_feature_names"] == ["sf_dispersion_5d_neg"]
    assert projection["all_raw_inactive_values_exact_zero"]
    np.testing.assert_array_equal(projected, full[:, :19])
    np.testing.assert_array_equal(subject.project_validation(raw[:3], parameters, projection), projected[:3])
    assert subject.project_validation(np.empty((0, 20)), parameters, projection).shape == (0, 19)


@pytest.mark.parametrize("bad", [1e-300, 1.0, float("nan"), float("inf")])
def test_d1_no_near_zero_nonzero_or_nonfinite_allowlist(bad):
    raw, parameters = _projection_fixture()
    raw[0, 19] = bad
    with pytest.raises(subject.FormalStateError):
        subject.project_training(
            raw, parameters, family=subject.FAMILIES[1], level="L2", sector="801207.SI", source_receipt_sha256="a" * 64
        )


def test_d1_no_dynamic_projection_for_other_sector_or_level():
    raw, parameters = _projection_fixture()
    for level, sector in (("L2", "801206.SI"), ("L1", "801207.SI")):
        values, projection = subject.project_training(
            raw, parameters, family=subject.FAMILIES[1], level=level, sector=sector, source_receipt_sha256="a" * 64
        )
        assert values.shape == (120, 20) and projection["inactive_feature_indices"] == []
        with pytest.raises(subject.FormalStateError, match="sector-local variance"):
            subject.initialize(values, 42)
    rng = np.random.RandomState(43)
    full = rng.normal(size=(120, 20))
    identity_values, identity = subject.project_training(
        full, parameters, family=subject.FAMILIES[1], level="L2", sector="801206.SI", source_receipt_sha256="b" * 64
    )
    assert identity["likelihood_feature_count"] == 20 and identity["inactive_feature_indices"] == []
    np.testing.assert_array_equal(identity_values, subject.preprocess_apply(full, parameters))


@pytest.mark.parametrize("drift", ["mask", "algorithm", "preprocess", "validation", "full_features"])
def test_d1_rehashed_drift_does_not_authorize_projection(drift):
    raw, parameters = _projection_fixture()
    _, projection = subject.project_training(
        raw, parameters, family=subject.FAMILIES[1], level="L2", sector="801207.SI", source_receipt_sha256="a" * 64
    )
    raw = raw[:3].copy()
    if drift == "mask":
        projection["active_feature_mask"][0] = 1
    elif drift == "algorithm":
        projection["algorithm_version"] = "unknown"
    elif drift == "preprocess":
        parameters = {**parameters, "center": [v + 0.1 for v in parameters["center"]]}
    elif drift == "validation":
        raw[0, 19] = 1e-300
    else:
        raw = raw[:, :19]
    projection = subject.receipt({k: v for k, v in projection.items() if k != "receipt_sha256"})
    with pytest.raises(subject.FormalStateError):
        subject.project_validation(raw, parameters, projection)


def test_d5_mixed_dimension_uses_effective_dimension_not_global_twenty():
    candidates = _selection_candidates()
    for candidate in candidates:
        candidate["entries"]["a"].update(final_train_likelihood=-1900.0, training_rows=100, feature_count=19)
    result = subject.select_restart(candidates, ["a"])
    assert result["selected_seed"] == 42
    assert result["candidates"][0]["scores"] == [-1.0]


@pytest.mark.parametrize(
    "field", ["source_profile_receipt_sha256", "projected_matrix_sha256", "active_feature_indices"]
)
def test_parent_rejects_rehashed_child_projection_before_d5(monkeypatch, field):
    raw, _ = _projection_fixture()
    other = np.random.RandomState(43).normal(size=(120, 20))
    preprocess = subject.preprocess_fit([other, raw], subject.FAMILIES[1])
    codes = ["801206.SI", "801207.SI"]
    series = {
        code: {"train_values": values.tolist(), "train_dates": ["2022-01-03"] * 120, "source_receipt_sha256": "a" * 64}
        for code, values in zip(codes, (other, raw))
    }
    entries = {}
    for code in codes:
        _, projection = subject.project_training(
            np.asarray(series[code]["train_values"]),
            preprocess,
            family=subject.FAMILIES[1],
            level="L2",
            sector=code,
            source_receipt_sha256="a" * 64,
        )
        if code == "801207.SI":
            projection[field] = list(range(20)) if field == "active_feature_indices" else "b" * 64
            projection = subject.receipt({k: v for k, v in projection.items() if k != "receipt_sha256"})
        entries[code] = subject.receipt(
            {"accepted": False, "reasons": ["hmm_risk_model_fit_failed"], "projection": projection}
        )
    key = f"{subject.FAMILIES[1]}:L2"
    groups = {
        key: {"preprocess": preprocess, "candidates": [{"seed": seed, "entries": entries} for seed in subject.SEEDS]}
    }
    groups.update(
        {f"{family}:{level}": {} for family in subject.FAMILIES for level in ("L1", "L2") if f"{family}:{level}" != key}
    )
    repeat = subject.receipt(
        {
            "request_sha256": "c" * 64,
            "fit_attempts": 2592,
            "selection_performed": False,
            "validation_accessed": False,
            "groups": groups,
        }
    )
    monkeypatch.setattr(executor, "validate_fit_entry", lambda *_: None)
    monkeypatch.setattr(executor, "select_restart", lambda *_: pytest.fail("D5 accessed before projection closure"))
    with pytest.raises(subject.FormalStateError, match="projection authority differs"):
        executor.finalize(
            {"receipt_sha256": "c" * 64, "sector_codes": {"L2": codes}, "series": {key: series}}, repeat, repeat
        )


def test_selected_artifact_preserves_mixed_shape_and_zero_refit(monkeypatch):
    # Synthetic serialization only, not a source/D3-D6 acceptance claim.
    codes = {
        "L1": [f"801{i:03}.SI" for i in range(31)],
        "L2": sorted([f"802{i:03}.SI" for i in range(130)] + ["801207.SI"]),
    }
    request = {
        "receipt_sha256": "a" * 64,
        "sector_codes": codes,
        "series": {},
        "source_identity": {},
        "industry_authority": {},
        "policy": {},
    }
    groups, selections, semantic = {}, {}, {}
    rng = np.random.RandomState(44)
    for family in subject.FAMILIES:
        dimension = 7 if family == subject.FAMILIES[0] else 20
        raw = rng.normal(size=(4, dimension))
        special = raw.copy()
        special[:, -1] = 0
        preprocess = subject.preprocess_fit([raw, special], family)
        for level in ("L1", "L2"):
            key = f"{family}:{level}"
            entries, source, meanings = {}, {}, {}
            for code in codes[level]:
                values = special if (family, level, code) == (subject.FAMILIES[1], "L2", "801207.SI") else raw
                _, projection = subject.project_training(
                    values, preprocess, family=family, level=level, sector=code, source_receipt_sha256="b" * 64
                )
                d = projection["likelihood_feature_count"]
                entries[code] = subject.receipt(
                    {
                        "accepted": True,
                        "projection": projection,
                        "feature_count": d,
                        "model": {
                            "startprob": [1 / 3] * 3,
                            "transmat": np.eye(3).tolist(),
                            "means": np.zeros((3, d)).tolist(),
                            "covariance": np.ones((3, d)).tolist(),
                        },
                    }
                )
                source[code] = {"train_values": values.tolist(), "source_receipt_sha256": "b" * 64}
                meanings[code] = {"assignment_status": "accepted", "evidence_status": "accepted"}
            request["series"][key] = source
            groups[key] = {"preprocess": preprocess, "candidates": [{"seed": 42, "entries": entries}]}
            selections[key] = {"accepted": True, "selected_seed": 42}
            semantic[key] = meanings
    final = subject.receipt(
        {
            "d3_d6_accepted": True,
            "request_sha256": request["receipt_sha256"],
            "selection": selections,
            "semantic": semantic,
        }
    )
    payload = executor.selected_model_set(final, request, groups)
    mixed = payload["selected_models"][f"{subject.FAMILIES[1]}:L2"]
    assert mixed["likelihood_feature_count_histogram"] == {"19": 1, "20": 130}
    assert np.asarray(mixed["models"]["801207.SI"]["model"]["means"]).shape == (3, 19)
    assert len(mixed["models"]["801207.SI"]["feature_names"]) == 20
    assert payload["ready"] is payload["phase2_ready"] is False
    monkeypatch.setattr(executor, "validate_semantic_readback", lambda *_: None)
    monkeypatch.setattr(executor, "fit_entry", lambda *_: pytest.fail("unexpected fit"))
    monkeypatch.setattr(executor, "select_restart", lambda *_: pytest.fail("unexpected selection"))
    executor.validate_selected_model_set(payload, final, request, groups)
    changed = copy.deepcopy(payload)
    changed["selected_models"][f"{subject.FAMILIES[1]}:L2"]["models"]["801207.SI"]["feature_names"].reverse()
    changed = subject.receipt({k: v for k, v in changed.items() if k != "receipt_sha256"})
    with pytest.raises(subject.FormalStateError, match="source/readback differs"):
        executor.validate_selected_model_set(changed, final, request, groups)


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
