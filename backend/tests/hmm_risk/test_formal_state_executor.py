from __future__ import annotations

import copy
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pytest

from backend.services.hmm_risk import formal_state_executor as executor
from backend.services.hmm_risk import formal_state_model as subject
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.tests.hmm_risk import test_formal_state_model as numerical_test_support

fitted = numerical_test_support.fitted
single_thread_numerics = numerical_test_support.single_thread_numerics
_projection_fixture = numerical_test_support._projection_fixture
_selection_candidates = numerical_test_support._selection_candidates


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
            "schema_version": subject.VERSION,
            "request_sha256": "c" * 64,
            "numeric_environment": {"test_only": True},
            "fit_attempts": 2592,
            "selection_performed": False,
            "validation_accessed": False,
            "ready": False,
            "groups": groups,
        }
    )
    monkeypatch.setattr(executor, "numeric_environment", lambda: {"test_only": True})
    monkeypatch.setattr(executor, "validate_fit_entry", lambda *_, **__: None)
    monkeypatch.setattr(executor, "select_restart", lambda *_: pytest.fail("D5 accessed before projection closure"))
    with pytest.raises(subject.FormalStateError, match="projection authority differs"):
        executor.finalize(
            {"receipt_sha256": "c" * 64, "sector_codes": {"L2": codes}, "series": {key: series}, "train_calendar": []},
            repeat,
            repeat,
        )


def test_selected_artifact_preserves_mixed_shape_and_zero_refit(monkeypatch):
    # Synthetic serialization only, not a source/D3-D6 acceptance claim.
    validated = []
    monkeypatch.setattr(executor, "validate_fit_entry", lambda *args, **kwargs: validated.append((args, kwargs)))
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
        "train_calendar": ["2022-01-03", "2022-01-04", "2022-01-05", "2022-01-06"],
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
                source[code] = {
                    "train_values": values.tolist(),
                    "train_dates": request["train_calendar"],
                    "source_receipt_sha256": "b" * 64,
                }
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
    assert len(validated) == 324
    assert all(kwargs["calendar"] == request["train_calendar"] for _, kwargs in validated)
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


def test_train_date_receipt_preserves_calendar_gaps_without_inventing_states():
    calendar = ["2022-01-03", "2022-01-04", "2022-01-05", "2022-01-06"]
    observed = [calendar[0], calendar[2], calendar[3]]
    value = subject.train_structure(np.eye(3), observed, calendar)
    dates = value["date_receipt"]
    assert dates["missing_dates"] == [calendar[1]] and dates["missing_date_count"] == 1
    assert dates["N_train"] == 3 and dates["run_basis"] == "immutable_observation_rows_not_calendar_contiguity"
    assert sum(state["incoming"] for state in value["states"]) == 2
    for invalid in ([calendar[0]] * 2, ["2021-12-31"], []):
        with pytest.raises(subject.FormalStateError, match="train dates"):
            subject.train_date_receipt(invalid, calendar)


def test_full_grid_failures_remain_complete_and_never_access_d6(monkeypatch):
    # Control-flow budget test: mocked fits are not formal training evidence.
    codes = {
        "L1": [f"801{i:03}.SI" for i in range(31)],
        "L2": sorted([f"802{i:03}.SI" for i in range(130)] + ["801207.SI"]),
    }
    dates = [(date(2022, 1, 3) + timedelta(days=i)).isoformat() for i in range(120)]
    request = {"receipt_sha256": "c" * 64, "sector_codes": codes, "train_calendar": dates, "series": {}}
    rng = np.random.RandomState(42)
    for family in subject.FAMILIES:
        raw = rng.normal(size=(120, 7 if family == subject.FAMILIES[0] else 20))
        for level in ("L1", "L2"):
            series = {}
            for code in codes[level]:
                values = raw.copy()
                if (family, level, code) == (subject.FAMILIES[1], "L2", "801207.SI"):
                    values[:, 19] = 0
                series[code] = {
                    "train_values": values.tolist(),
                    "train_dates": dates,
                    "source_receipt_sha256": "a" * 64,
                }
            request["series"][f"{family}:{level}"] = series
    attempts = []

    def failed_fit(values, observed, seed, *, calendar):
        attempts.append((seed, values.shape[1]))
        assert calendar == observed == dates
        raise subject.FormalStateError("hmm_risk_model_initialization_failed", "synthetic test failure")

    monkeypatch.setattr(executor, "fit_entry", failed_fit)
    monkeypatch.setattr(executor, "numeric_environment", lambda: {"test_only": True})
    monkeypatch.setattr(executor, "evaluate_calendar_evidence", lambda *_, **__: pytest.fail("D6 accessed"))
    repeat = executor.train_repeat(request)
    assert len(attempts) == repeat["fit_attempts"] == 2592
    assert all(sum(seed == s for s, _ in attempts) == 324 for seed in subject.SEEDS)
    assert sum(d == 19 for _, d in attempts) == 8
    result = executor.finalize(request, repeat, copy.deepcopy(repeat))
    assert result["fit_attempts"] == 5184 and not result["d3_d6_accepted"]
    assert not result["semantic"] and all(not item["accepted"] for item in result["selection"].values())
    assert result["ready"] is result["phase2_ready"] is False


def test_parent_selected_d6_sparse_evidence_real_readback_without_refit(monkeypatch):
    # Synthetic numerical fixture + mocked grid execution, never a formal fit.
    # Parent D4/D5, selected D6 and semantic readback below are not mocked.
    from backend.services.hmm_risk.formal_state_calendar import build_calendar_carrier
    from backend.services.hmm_risk.contracts import BASE_FEATURES

    rng = np.random.RandomState(42)
    signal = np.tile(np.repeat([-3.0, 0.0, 3.0], 30), 6)
    raw = signal[:, None] + rng.normal(0, 0.4, (len(signal), 7))
    dates = [(date(2022, 1, 3) + timedelta(days=i)).isoformat() for i in range(len(raw))]
    projected, _ = subject.project_training(
        raw,
        subject.preprocess_fit([raw], subject.FAMILIES[0]),
        family=subject.FAMILIES[0],
        level="L1",
        sector="801000.SI",
        source_receipt_sha256="c" * 64,
    )
    fitted_entry = subject.fit_entry(projected, dates, 42, calendar=dates)
    assert fitted_entry["accepted"] is True
    codes = {"L1": [f"801{i:03}.SI" for i in range(31)], "L2": [f"802{i:03}.SI" for i in range(131)]}
    source_identity = {"test_only": "synthetic source, no dataset access"}
    policy = {"receipt_sha256": "a" * 64}
    carrier = build_calendar_carrier(
        dates=_validation_dates(),
        feature_names=BASE_FEATURES,
        observations=raw[:182],
        components={f"excess_return_{h}d": {"positions": [0, 1], "values": [0.01, 0.02]} for h in (5, 10, 20)},
        source_identity_sha256=canonical_sha256(source_identity),
        source_receipt_sha256=policy["receipt_sha256"],
    )
    request = {
        "receipt_sha256": "b" * 64,
        "sector_codes": codes,
        "train_calendar": dates,
        "validation_calendar": _validation_dates(),
        "source_identity": source_identity,
        "policy": policy,
        "series": {},
    }
    for family in subject.FAMILIES:
        values = raw if family == subject.FAMILIES[0] else rng.normal(size=(len(raw), 20))
        for level in ("L1", "L2"):
            request["series"][f"{family}:{level}"] = {
                code: {
                    "train_values": values.tolist(),
                    "train_dates": dates,
                    "feature_names": list(BASE_FEATURES) if family == subject.FAMILIES[0] else [],
                    "source_receipt_sha256": "c" * 64,
                    "validation": carrier,
                }
                for code in codes[level]
            }
    calls = 0

    def synthetic_grid_fit(values, observed, seed, *, calendar):
        nonlocal calls
        calls += 1
        if calls <= 248 and seed == 42:
            assert np.array_equal(values, raw) and observed == calendar == dates
            return fitted_entry
        raise subject.FormalStateError("hmm_risk_model_fit_failed", "test-only failed candidate")

    monkeypatch.setattr(executor, "numeric_environment", lambda: {"test_only": "fixed environment"})
    monkeypatch.setattr(executor, "fit_entry", synthetic_grid_fit)
    repeat = executor.train_repeat(request)
    assert calls == 2592
    monkeypatch.setattr(executor, "fit_entry", lambda *_, **__: pytest.fail("unexpected refit"))
    final = executor.finalize(request, repeat, copy.deepcopy(repeat))
    key = f"{subject.FAMILIES[0]}:L1"
    assert final["selection"][key]["selected_seed"] == 42
    assert set(final["semantic"]) == {key} and len(final["semantic"][key]) == 31
    for meaning in final["semantic"][key].values():
        assert meaning["assignment_status"] == "accepted" and meaning["evidence_status"] == "failed"
        assert meaning["primary_reason"] == "hmm_risk_semantic_validation_evidence_rows_insufficient"
        assert len(meaning["ledger"]) == len(meaning["posterior"]) == 182
    assert final["d3_d6_accepted"] is final["ready"] is False
    monkeypatch.setattr(executor, "select_restart", lambda *_: pytest.fail("unexpected D5 reselection"))
    executor.validate_semantic_readback(final, request, repeat["groups"])
    changed = copy.deepcopy(final)
    meaning = changed["semantic"][key][codes["L1"][0]]
    meaning["ledger"][0]["evidence_included"] = False
    changed["semantic"][key][codes["L1"][0]] = subject.receipt(
        {k: v for k, v in meaning.items() if k != "receipt_sha256"}
    )
    changed = subject.receipt({k: v for k, v in changed.items() if k != "receipt_sha256"})
    with pytest.raises(subject.FormalStateError, match="semantic write/readback differs"):
        executor.validate_semantic_readback(changed, request, repeat["groups"])
    with pytest.raises(subject.FormalStateError, match="accepted current request"):
        executor.selected_model_set(final, request, repeat["groups"])


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


@pytest.mark.parametrize(
    "field", ["numeric_environment", "schema_version", "ready", "validation_accessed", "fit_attempts"]
)
def test_parent_rejects_equal_rehashed_repeat_contract_drift_before_d5(monkeypatch, field):
    environment = {"test_only": "fixed parent/child identity"}
    monkeypatch.setattr(executor, "numeric_environment", lambda: environment)
    monkeypatch.setattr(executor, "select_restart", lambda *_: pytest.fail("D5 accessed before repeat closure"))
    body = {
        "schema_version": subject.VERSION,
        "request_sha256": "a" * 64,
        "numeric_environment": environment,
        "fit_attempts": 2592,
        "groups": {f"{family}:{level}": {} for family in subject.FAMILIES for level in ("L1", "L2")},
        "selection_performed": False,
        "validation_accessed": False,
        "ready": False,
    }
    body[field] = {
        "numeric_environment": {"test_only": "different host or versions"},
        "schema_version": "unknown",
        "ready": True,
        "validation_accessed": 0,
        "fit_attempts": 2592.0,
    }[field]
    repeat = subject.receipt(body)
    with pytest.raises(subject.FormalStateError, match="repeat (environment|contract)"):
        executor.finalize({"receipt_sha256": "a" * 64}, repeat, copy.deepcopy(repeat))


@pytest.mark.parametrize("stage", ["child", "finalize", "acceptance_readback", "model_readback"])
def test_parent_durable_failure_covers_children_and_finalization(tmp_path, monkeypatch, stage):
    request_file = tmp_path / "request.json"
    executor.write_once(request_file, {"test_only": True})
    request = {"source_identity": {"dataset_root": str(tmp_path / "release")}}
    monkeypatch.setattr(executor, "load_request", lambda *_: request)
    child_calls = []

    def child(command, **kwargs):
        child_calls.append(command)
        assert command[0] == sys.executable and "child" in command
        assert all(kwargs["env"][key] == "1" for key in executor.THREAD_VARIABLES)
        if stage == "child":
            return subprocess.CompletedProcess(command, 1)
        executor.write_once(Path(command[-1]), subject.receipt({"groups": {}}))
        return subprocess.CompletedProcess(command, 0)

    def reject(*_, **__):
        raise subject.FormalStateError("hmm_risk_model_receipt_invalid", f"test-only {stage} failure")

    monkeypatch.setattr(executor.subprocess, "run", child)
    monkeypatch.setattr(
        executor, "finalize", reject if stage == "finalize" else lambda *_: subject.receipt({"d3_d6_accepted": True})
    )
    monkeypatch.setattr(
        executor, "validate_semantic_readback", reject if stage == "acceptance_readback" else lambda *_: None
    )
    monkeypatch.setattr(executor, "selected_model_set", lambda *_: subject.receipt({"ready": False}))
    monkeypatch.setattr(executor, "validate_selected_model_set", reject)
    output = tmp_path / "output"
    script = Path(__file__).resolve().parents[3] / "scripts/hmm_risk/run_formal_state_model_set.py"
    with pytest.raises(subject.FormalStateError):
        executor.run_two_processes(request_file, output, script)
    failure = executor.read_json(output / "parent.failure.json")
    executor.verify_hash(failure)
    assert len(child_calls) == (1 if stage == "child" else 2)
    assert failure["status"] == "failed" and failure["ready"] is False
    assert failure["database_write"] is failure["runtime_action"] is False
    assert failure["request_file_sha256"] is not None


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


def test_l2_research_rejects_self_hashed_non_authoritative_source_without_fit_or_d5(monkeypatch):
    for name in ("fit_entry", "train_repeat", "select_restart"):
        monkeypatch.setattr(executor, name, lambda *_: pytest.fail("fit or D5 accessed"))
    request = subject.receipt({"source_identity": {"generation": executor.FROZEN_GENERATION}})
    original = subject.receipt({"request_sha256": request["receipt_sha256"]})
    with pytest.raises(subject.FormalStateError, match="approved frozen source"):
        executor.l2_research_readback(request, original)


def test_output_location_rejects_source_release_relative_and_indirect_paths(tmp_path, monkeypatch):
    root = Path(__file__).resolve().parents[3]
    release = tmp_path / "release"
    release.mkdir()
    (release / "direct_monthly_state.json").write_text("{}", encoding="utf-8")
    for path in (Path("relative.json"), root / "tmp/forbidden.json", release / "new/failure.json"):
        with pytest.raises(subject.FormalStateError, match="output"):
            executor.validate_output_location(path)
    link = tmp_path / "indirect"
    # Mock only the OS indirect-path probe: no admin-dependent symlink creation.
    monkeypatch.setattr(Path, "is_symlink", lambda path: path == link)
    with pytest.raises(subject.FormalStateError, match="indirect"):
        executor.validate_output_location(link / "failure.json")
    assert executor.validate_output_location(tmp_path / "result.json") == tmp_path / "result.json"
    assert not (release / "new").exists()
