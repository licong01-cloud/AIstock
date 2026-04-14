"""Property-based tests for OptunaHyperparamOptimizer.

Feature: p4-p1-p5-strategy-enhancement
Uses hypothesis to verify correctness properties of the Optuna TPE optimizer.
"""
from __future__ import annotations

import json
import os
import shutil
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest
from hypothesis import given, settings, assume, HealthCheck
from hypothesis import strategies as st

import optuna
from optuna.trial import TrialState

from backend.services.quantevolver.optuna_optimizer import (
    HYPERPARAM_RANGES,
    INTEGER_PARAMS,
    LOG_SCALE_PARAMS,
    OptunaHyperparamOptimizer,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_optimizer(tmp_path, task_id: str = "task_001", model_type: str = "LGB"):
    """Create an OptunaHyperparamOptimizer with storage in a temp directory."""
    opt = OptunaHyperparamOptimizer(task_id=task_id, model_type=model_type)
    studies_dir = os.path.join(str(tmp_path), "optuna_studies")
    os.makedirs(studies_dir, exist_ok=True)
    opt.storage_path = os.path.join(
        studies_dir, f"{task_id}_{model_type.upper()}.db"
    )
    return opt


def _patch_db_empty():
    """Return a patch context that makes DB queries return empty results."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = {"cnt": 0}
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    return patch(
        "backend.services.quantevolver.optuna_optimizer.OptunaHyperparamOptimizer._inject_historical_trials"
    ), patch(
        "backend.services.quantevolver.optuna_optimizer.OptunaHyperparamOptimizer._inject_cross_task_trials"
    )



# ---------------------------------------------------------------------------
# Property 12: Optuna Study 幂等创建
# Feature: p4-p1-p5-strategy-enhancement, Property 12: Optuna Study 幂等创建
# Validates: Requirements 7.1
# ---------------------------------------------------------------------------


@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    task_id=st.from_regex(r"task_[0-9]{3}", fullmatch=True),
    model_type=st.sampled_from(list(HYPERPARAM_RANGES.keys())),
)
def test_property_12_study_idempotent_creation(tmp_path, task_id: str, model_type: str):
    """Property 12: Optuna Study 幂等创建

    For same (task_id, model_type), calling get_or_create_study() twice
    should return Studies with the same trial count (no duplicate injection).

    **Validates: Requirements 7.1**
    """
    opt = _make_optimizer(tmp_path, task_id=task_id, model_type=model_type)

    # Mock out DB-dependent injection methods so they don't hit real DB
    with patch.object(opt, "_inject_historical_trials") as mock_hist, \
         patch.object(opt, "_inject_cross_task_trials") as mock_cross:

        study1 = opt.get_or_create_study()
        assert study1 is not None

        trial_count_1 = len(study1.trials)

        # Second call — should reuse the cached study, no re-injection
        study2 = opt.get_or_create_study()
        assert study2 is not None

        trial_count_2 = len(study2.trials)

        # Core assertion: trial count must be identical
        assert trial_count_1 == trial_count_2, (
            f"Trial count changed between calls: {trial_count_1} → {trial_count_2}. "
            f"Duplicate injection detected for ({task_id}, {model_type})."
        )

        # Injection methods should be called at most once (on first creation)
        assert mock_hist.call_count <= 1, (
            f"_inject_historical_trials called {mock_hist.call_count} times, expected ≤ 1"
        )
        assert mock_cross.call_count <= 1, (
            f"_inject_cross_task_trials called {mock_cross.call_count} times, expected ≤ 1"
        )



# ---------------------------------------------------------------------------
# Property 13: 历史 Trial 注入
# Feature: p4-p1-p5-strategy-enhancement, Property 13: 历史 Trial 注入
# Validates: Requirements 7.2, 7.3
# ---------------------------------------------------------------------------


def _make_historical_rows(n: int, model_type: str) -> List[Dict[str, Any]]:
    """Build N mock qe_evolution_loops rows with param_tune records.

    Each row has config_json with model_params matching the model_type's
    HYPERPARAM_RANGES, and metrics_json with an IC value.
    """
    import random
    ranges = HYPERPARAM_RANGES.get(model_type, {})
    rows = []
    for i in range(n):
        model_params = {}
        for param_name, (lo, hi) in ranges.items():
            if param_name in INTEGER_PARAMS:
                model_params[param_name] = random.randint(int(lo), int(hi))
            else:
                model_params[param_name] = lo + random.random() * (hi - lo)

        ic_value = round(random.uniform(0.01, 0.15), 6)
        rows.append({
            "config_json": {"model_params": model_params},
            "metrics_json": {"IC": ic_value},
        })
    return rows


@settings(
    max_examples=50,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    n_records=st.integers(min_value=1, max_value=20),
    model_type=st.sampled_from(list(HYPERPARAM_RANGES.keys())),
)
def test_property_13_historical_trial_injection(tmp_path, n_records: int, model_type: str):
    """Property 13: 历史 Trial 注入

    Mock qe_evolution_loops with N param_tune records, verify new Study
    contains at least N completed trials with correct IC values.

    **Validates: Requirements 7.2, 7.3**
    """
    task_id = "task_hist"
    opt = _make_optimizer(tmp_path, task_id=task_id, model_type=model_type)

    rows = _make_historical_rows(n_records, model_type)
    expected_ics = [r["metrics_json"]["IC"] for r in rows]

    # Create a fresh in-memory study to inject into
    study = optuna.create_study(
        study_name=f"{task_id}_{model_type}",
        sampler=optuna.samplers.TPESampler(),
        direction="maximize",
    )

    # Mock the DB query inside _inject_historical_trials
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = rows
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    with patch(
        "backend.db.pg_pool.get_conn",
        return_value=mock_conn,
    ):
        opt._inject_historical_trials(study)

    completed_trials = [
        t for t in study.trials if t.state == TrialState.COMPLETE
    ]

    # Core assertion 1: at least N completed trials injected
    assert len(completed_trials) >= n_records, (
        f"Expected at least {n_records} completed trials, got {len(completed_trials)}. "
        f"model_type={model_type}"
    )

    # Core assertion 2: each injected trial's objective value matches an IC
    injected_ics = sorted([t.value for t in completed_trials])
    expected_ics_sorted = sorted(expected_ics)

    for expected_ic in expected_ics_sorted:
        # Find a matching IC in injected trials (within floating point tolerance)
        matched = any(
            abs(injected_ic - expected_ic) < 1e-6
            for injected_ic in injected_ics
        )
        assert matched, (
            f"Expected IC value {expected_ic} not found in injected trials. "
            f"Injected ICs: {injected_ics}"
        )



# ---------------------------------------------------------------------------
# Property 14: 跨 Task Trial 注入与上限
# Feature: p4-p1-p5-strategy-enhancement, Property 14: 跨 Task Trial 注入与上限
# Validates: Requirements 7.4, 10.1, 10.2, 10.3
# ---------------------------------------------------------------------------


def _make_cross_task_rows(n: int, model_type: str) -> List[Dict[str, Any]]:
    """Build N mock cross-task rows with source_task_id, config_json, metrics_json."""
    import random
    ranges = HYPERPARAM_RANGES.get(model_type, {})
    rows = []
    for i in range(n):
        model_params = {}
        for param_name, (lo, hi) in ranges.items():
            if param_name in INTEGER_PARAMS:
                model_params[param_name] = random.randint(int(lo), int(hi))
            else:
                model_params[param_name] = lo + random.random() * (hi - lo)

        ic_value = round(random.uniform(0.01, 0.15), 6)
        rows.append({
            "source_task_id": f"other_task_{i:03d}",
            "config_json": {"model_params": model_params, "model_id": f"{model_type}_v{i}"},
            "metrics_json": {"IC": ic_value},
        })
    return rows


@settings(
    max_examples=30,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    total_available=st.integers(min_value=51, max_value=100),
    model_type=st.sampled_from(list(HYPERPARAM_RANGES.keys())),
)
def test_property_14_cross_task_trial_injection_cap(
    tmp_path, total_available: int, model_type: str
):
    """Property 14: 跨 Task Trial 注入与上限

    Generate >50 cross-task trials, verify injection doesn't exceed 20,
    each has source_task_id in user_attrs.

    **Validates: Requirements 7.4, 10.1, 10.2, 10.3**
    """
    task_id = "task_cross"
    opt = _make_optimizer(tmp_path, task_id=task_id, model_type=model_type)

    # Sort by IC descending and take top 20 (simulating the SQL ORDER BY + LIMIT)
    all_rows = _make_cross_task_rows(total_available, model_type)
    all_rows.sort(key=lambda r: r["metrics_json"]["IC"], reverse=True)
    top_20_rows = all_rows[:20]

    # Create a fresh in-memory study
    study = optuna.create_study(
        study_name=f"{task_id}_{model_type}",
        sampler=optuna.samplers.TPESampler(),
        direction="maximize",
    )

    # Mock the DB queries: first call returns count, second returns top 20 rows
    mock_cursor = MagicMock()
    # fetchone for COUNT query
    mock_cursor.fetchone.return_value = {"cnt": total_available}
    # fetchall for the actual rows query — returns top 20
    mock_cursor.fetchall.return_value = top_20_rows
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    with patch(
        "backend.db.pg_pool.get_conn",
        return_value=mock_conn,
    ):
        opt._inject_cross_task_trials(study)

    completed_trials = [
        t for t in study.trials if t.state == TrialState.COMPLETE
    ]

    # Core assertion 1: injection count ≤ 20
    assert len(completed_trials) <= 20, (
        f"Injected {len(completed_trials)} cross-task trials, expected ≤ 20. "
        f"total_available={total_available}, model_type={model_type}"
    )

    # Core assertion 2: each injected trial has source_task_id in user_attrs
    for trial in completed_trials:
        assert "source_task_id" in trial.user_attrs, (
            f"Trial {trial.number} missing source_task_id in user_attrs. "
            f"user_attrs={trial.user_attrs}"
        )
        assert trial.user_attrs["source_task_id"].startswith("other_task_"), (
            f"Trial {trial.number} has unexpected source_task_id: "
            f"{trial.user_attrs['source_task_id']}"
        )



# ---------------------------------------------------------------------------
# Property 15: Optuna ask() 参数范围有效性
# Feature: p4-p1-p5-strategy-enhancement, Property 15: Optuna ask() 参数范围有效性
# Validates: Requirements 8.1, 8.2, 8.5
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("model_type", list(HYPERPARAM_RANGES.keys()))
def test_property_15_ask_param_ranges_valid(tmp_path, model_type: str):
    """Property 15: Optuna ask() 参数范围有效性

    For all model_types in HYPERPARAM_RANGES, call ask(), verify each param
    is within (min, max) range, integer params are int, float params are float.

    **Validates: Requirements 8.1, 8.2, 8.5**
    """
    opt = _make_optimizer(tmp_path, task_id="task_ask", model_type=model_type)

    # Mock out injection methods to avoid DB calls
    with patch.object(opt, "_inject_historical_trials"), \
         patch.object(opt, "_inject_cross_task_trials"):

        # Call ask() multiple times to get a spread of suggestions
        ranges = HYPERPARAM_RANGES[model_type]

        for iteration in range(20):
            result = opt.ask()
            assert result is not None, (
                f"ask() returned None for model_type={model_type}, iteration={iteration}"
            )

            trial, suggested_params = result

            # Verify all expected params are present
            for param_name in ranges:
                assert param_name in suggested_params, (
                    f"Missing param {param_name} in suggested_params for "
                    f"model_type={model_type}. Got: {list(suggested_params.keys())}"
                )

            # Verify each param is within range and has correct type
            for param_name, value in suggested_params.items():
                lo, hi = ranges[param_name]

                if param_name in INTEGER_PARAMS:
                    # Integer params must be int type
                    assert isinstance(value, int), (
                        f"Param {param_name}={value} should be int, "
                        f"got {type(value).__name__} for model_type={model_type}"
                    )
                    assert int(lo) <= value <= int(hi), (
                        f"Param {param_name}={value} out of range "
                        f"[{int(lo)}, {int(hi)}] for model_type={model_type}"
                    )
                else:
                    # Float params must be float (or int that can be treated as float)
                    assert isinstance(value, (float, int)), (
                        f"Param {param_name}={value} should be float, "
                        f"got {type(value).__name__} for model_type={model_type}"
                    )
                    float_val = float(value)
                    # For log-scale params, the lower bound may be adjusted to 1e-10
                    actual_lo = max(lo, 1e-10) if param_name in LOG_SCALE_PARAMS else lo
                    assert actual_lo <= float_val <= float(hi) + 1e-9, (
                        f"Param {param_name}={float_val} out of range "
                        f"[{actual_lo}, {hi}] for model_type={model_type}"
                    )

            # Tell the study a result so TPE has data for next iteration
            study = opt.get_or_create_study()
            study.tell(trial, 0.05)



# ---------------------------------------------------------------------------
# Property 16: Optuna tell() 反馈更新
# Feature: p4-p1-p5-strategy-enhancement, Property 16: Optuna tell() 反馈更新
# Validates: Requirements 8.4
# ---------------------------------------------------------------------------


@settings(
    max_examples=50,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    ic_value=st.floats(
        min_value=-0.5, max_value=0.5,
        allow_nan=False, allow_infinity=False,
    ),
    model_type=st.sampled_from(list(HYPERPARAM_RANGES.keys())),
)
def test_property_16_tell_feedback_update(tmp_path, ic_value: float, model_type: str):
    """Property 16: Optuna tell() 反馈更新

    Call ask() then tell(trial, ic_value), verify Study completed trial
    count increases by 1 and latest trial's objective equals ic_value.

    **Validates: Requirements 8.4**
    """
    opt = _make_optimizer(tmp_path, task_id="task_tell", model_type=model_type)

    # Mock out injection methods to avoid DB calls
    with patch.object(opt, "_inject_historical_trials"), \
         patch.object(opt, "_inject_cross_task_trials"):

        # Get the study and count completed trials before
        study = opt.get_or_create_study()
        assert study is not None

        completed_before = len([
            t for t in study.trials if t.state == TrialState.COMPLETE
        ])

        # ask() to get a trial
        result = opt.ask()
        assert result is not None, f"ask() returned None for model_type={model_type}"
        trial, suggested_params = result

        # tell() to report the IC value
        success = opt.tell(trial, ic_value)
        assert success is True, f"tell() returned False for model_type={model_type}"

        # Count completed trials after
        completed_after = len([
            t for t in study.trials if t.state == TrialState.COMPLETE
        ])

        # Core assertion 1: completed trial count increased by exactly 1
        assert completed_after == completed_before + 1, (
            f"Completed trial count did not increase by 1: "
            f"before={completed_before}, after={completed_after}. "
            f"model_type={model_type}"
        )

        # Core assertion 2: the latest completed trial's objective equals ic_value
        completed_trials = [
            t for t in study.trials if t.state == TrialState.COMPLETE
        ]
        latest_trial = max(completed_trials, key=lambda t: t.number)
        assert abs(latest_trial.value - ic_value) < 1e-9, (
            f"Latest trial objective {latest_trial.value} != expected {ic_value}. "
            f"model_type={model_type}"
        )
