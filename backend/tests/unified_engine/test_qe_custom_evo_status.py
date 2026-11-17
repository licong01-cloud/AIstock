from copy import deepcopy
from typing import Any

import pytest

import backend.services.quantevolver.qe_evolution_service as qes
from backend.services.quantevolver.qe_evolution_service import (
    AutoEvolutionScheduler,
    derive_custom_evo_final_status,
)
from backend.services.quantevolver.config_composer import ConfigComposer
from backend.services.quantevolver.qe_custom_evo_outcome import resolve_custom_evo_attempts


def test_custom_evo_final_status_requires_all_configured_loops_completed() -> None:
    status = derive_custom_evo_final_status(10, {"completed": 8, "failed": 2})

    assert status == "failed"


def test_custom_evo_final_status_completed_only_for_exact_full_success() -> None:
    status = derive_custom_evo_final_status(10, {"completed": 10, "failed": 0})

    assert status == "completed"


def test_custom_evo_final_status_fails_when_loops_missing() -> None:
    status = derive_custom_evo_final_status(10, {"completed": 8})

    assert status == "failed"


def test_custom_evo_final_status_fails_when_any_loop_is_canceled() -> None:
    status = derive_custom_evo_final_status(10, {"completed": 9, "canceled": 1})

    assert status == "failed"


def test_custom_evo_final_status_fails_when_extra_active_loop_exists() -> None:
    status = derive_custom_evo_final_status(10, {"completed": 10, "running": 1})

    assert status == "failed"


def test_recompute_custom_evo_task_status_reads_real_dict_rows(monkeypatch) -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.query = ""
            self.updated_status: tuple[str, str] | None = None

        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

        def execute(self, query: str, params: tuple[Any, ...]) -> None:
            self.query = query
            if query.startswith("UPDATE qe_evolution_tasks"):
                self.updated_status = (str(params[0]), str(params[1]))

        def fetchone(self) -> dict[str, Any]:
            assert "SELECT strategy_evo_config" in self.query
            return {"strategy_evo_config": {"loops": [{}, {}]}}

        def fetchall(self) -> list[dict[str, Any]]:
            assert "metrics_json -> 'absolute_target_result'" in self.query
            return [{"loop_index": index, "status": "completed"} for index in (1, 2)]

    class FakeConnection:
        def __init__(self) -> None:
            self.cursor_instance = FakeCursor()
            self.committed = False

        def __enter__(self) -> "FakeConnection":
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

        def cursor(self, **_kwargs: Any) -> FakeCursor:
            return self.cursor_instance

        def commit(self) -> None:
            self.committed = True

    connection = FakeConnection()
    monkeypatch.setattr(qes, "get_conn", lambda: connection)
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    scheduler._parse_custom_evo_strategy_config = lambda *_args, **_kwargs: {
        "loops": [{}, {}]
    }

    status = scheduler.recompute_custom_evo_task_status("qe_test")

    assert status == "completed"
    assert connection.cursor_instance.updated_status == ("completed", "qe_test")
    assert connection.committed is True


def _target_loop(arm="B"):
    return {
        "absolute_target_replay": {
            "schema_version": "qe_absolute_target_replay_v1", "arm": arm,
            "targets": {"path": "targets.parquet", "sha256": "a" * 64},
            "dataset": {"release_id": "frozen", "manifest_sha256": "b" * 64, "profile_sha256": "c" * 64},
            "window": {"signal_start": "2024-07-01", "signal_end": "2026-10-28",
                       "account_start": "2024-07-02", "account_end": "2026-10-29", "account_days": 564},
            "initial_cash": 10000000,
            "execution_reference": {"path": "conf.yaml", "sha256": "d" * 64},
            "sector_ids": [0, 1], "known_at": "UNVERIFIED",
        },
        "execution_algo": "TWAP", "execution_algo_params": {"unfilled_handler": "TAIL_BOOST"},
        "strategy_params": {"initial_cash": 10000000, "risk_policy": {"enabled": True}},
    }


def _attempt(config, index, status):
    contract = config["absolute_target_replay"]
    receipt = {
        "schema_version": contract["schema_version"], "arm": contract["arm"],
        "status": "COMPLETE_ACCOUNT_AND_TARGET_CONSUMPTION",
        "targets_sha256": contract["targets"]["sha256"],
        "dataset": deepcopy(contract["dataset"]), "window": deepcopy(contract["window"]),
        "account_days": 564, "target_days_consumed": 564,
        "new_fit_calls": 0, "new_predict_calls": 0, "IC": None, "RankIC": None,
        "label_status": "NOT_APPLICABLE", "known_at": "UNVERIFIED", "actions_sha256": "e" * 64,
    }
    return {"loop_index": index, "status": status, "config_json": deepcopy(config),
            "experiment_id": f"experiment-{index}" if status == "completed" else None,
            "metrics_json": {"absolute_target_result": receipt} if status == "completed" else None}


def test_original_two_arms_succeed_without_erasing_four_failed_attempts():
    configs = [_target_loop(arm) for arm in ("T", "B", "T", "B", "B", "B")]
    rows = [_attempt(cfg, index, "completed" if index in (3, 6) else "failed")
            for index, cfg in enumerate(configs, 1)]
    before = deepcopy((configs, rows))
    outcome = resolve_custom_evo_attempts(configs, rows)
    assert outcome["effective_loop_indexes"] == [3, 6]
    assert outcome["recovered_attempts"] == [
        {"loop_index": 1, "resolved_by_loop_index": 3},
        *({"loop_index": index, "resolved_by_loop_index": 6} for index in (2, 4, 5)),
    ]
    assert derive_custom_evo_final_status(outcome["expected_count"], outcome["status_counts"]) == "completed"
    assert outcome["historical_status_counts"] == {"failed": 4, "completed": 2}
    assert (configs, rows) == before


@pytest.mark.parametrize("field", ["arm", "targets", "dataset", "window", "initial_cash", "execution_reference"])
def test_changed_frozen_contract_cannot_resolve_an_earlier_failure(field):
    old, new = _target_loop(), _target_loop()
    changed = {"arm": "T", "targets": {"path": "targets.parquet", "sha256": "f" * 64},
               "dataset": {"release_id": "another", "manifest_sha256": "f" * 64, "profile_sha256": "c" * 64},
               "window": {**new["absolute_target_replay"]["window"], "account_days": 563},
               "initial_cash": 20000000,
               "execution_reference": {"path": "conf.yaml", "sha256": "f" * 64}}
    new["absolute_target_replay"][field] = changed[field]
    rows = [_attempt(old, 1, "failed"), _attempt(new, 2, "completed")]
    outcome = resolve_custom_evo_attempts([old, new], rows)
    assert outcome["recovered_attempts"] == []
    assert derive_custom_evo_final_status(outcome["expected_count"], outcome["status_counts"]) == "failed"


@pytest.mark.parametrize("field,value", [
    ("strategy_params", {"initial_cash": 10000000, "risk_policy": {"enabled": False}}),
    ("execution_algo", "VWAP"), ("execution_algo_params", {"unfilled_handler": "NONE"}),
    ("stock_pool", "csi300"), ("suspend_filter_strict", False),
])
def test_changed_trading_settings_are_independent_accounts(field, value):
    old, new = _target_loop(), _target_loop()
    new[field] = value
    outcome = resolve_custom_evo_attempts([old, new], [_attempt(old, 1, "failed"), _attempt(new, 2, "completed")])
    assert outcome["recovered_attempts"] == []
    assert outcome["status_counts"] == {"failed": 1, "completed": 1}


@pytest.mark.parametrize("corruption", ["missing_receipt", "partial_window", "wrong_hash", "missing_experiment", "snapshot_drift"])
def test_incomplete_or_mismatched_success_cannot_hide_failure(corruption):
    cfg = _target_loop()
    rows = [_attempt(cfg, 1, "failed"), _attempt(cfg, 2, "completed")]
    if corruption == "missing_receipt":
        rows[1]["metrics_json"] = {}
    elif corruption == "partial_window":
        rows[1]["metrics_json"]["absolute_target_result"]["account_days"] = 563
    elif corruption == "wrong_hash":
        rows[1]["metrics_json"]["absolute_target_result"]["targets_sha256"] = "f" * 64
    elif corruption == "missing_experiment":
        rows[1]["experiment_id"] = None
    else:
        rows[1]["config_json"]["strategy_params"]["initial_cash"] = 1
    outcome = resolve_custom_evo_attempts([cfg, cfg], rows)
    assert outcome["recovered_attempts"] == []
    assert outcome["unverified_completed_loop_indexes"] == [2]


def test_missing_running_cancelled_and_later_failed_attempts_are_not_discarded():
    cfg = _target_loop()
    for state in ("running", "cancelled", "failed"):
        outcome = resolve_custom_evo_attempts([cfg, cfg], [_attempt(cfg, 1, state), _attempt(cfg, 2, "completed")])
        if state == "failed":
            assert outcome["recovered_attempts"]
        else:
            assert outcome["recovered_attempts"] == []
            assert state in outcome["status_counts"]
    outcome = resolve_custom_evo_attempts([cfg, cfg], [_attempt(cfg, 1, "completed"), _attempt(cfg, 2, "failed")])
    assert outcome["recovered_attempts"] == []
    outcome = resolve_custom_evo_attempts([cfg, cfg], [_attempt(cfg, 2, "completed")])
    assert outcome["expected_count"] == 2
    assert outcome["recovered_attempts"] == []


def test_same_label_or_model_config_is_never_implicitly_a_retry():
    cfg = {"label": "same", "model_id": "lgb", "model_params": {"seed": 42}}
    outcome = resolve_custom_evo_attempts([cfg, cfg], [{"loop_index": 1, "status": "failed"}, {"loop_index": 2, "status": "completed"}])
    assert outcome["expected_count"] == 2
    assert outcome["status_counts"] == {"failed": 1, "completed": 1}


def test_real_serializer_execution_params_are_verified_instead_of_treated_as_model_hyperparameters():
    from backend.services.quantevolver.experiment_config_builders import build_config_from_custom_evo_loop
    from backend.services.quantevolver.runtime_contract import build_qe_minute_runtime_contract

    cfg = _target_loop()
    planned = build_config_from_custom_evo_loop(cfg, {})
    rows = [_attempt(cfg, 1, "failed"), _attempt(cfg, 2, "completed")]
    runtime = build_qe_minute_runtime_contract(
        execution_algo=planned.execution_algo, execution_algo_params=planned.execution_algo_params,
        source="test_real_config_record", require_minute=True,
    )
    for row in rows:
        row["config_json"].update(runtime)
        row["config_json"]["model_params"] = {**planned.build_custom_params(), **runtime}
    outcome = resolve_custom_evo_attempts([cfg, cfg], rows)
    assert outcome["effective_loop_indexes"] == [2]
    rows[1]["config_json"]["model_params"]["learning_rate"] = 0.1
    outcome = resolve_custom_evo_attempts([cfg, cfg], rows)
    assert outcome["recovered_attempts"] == []
    assert outcome["unverified_completed_loop_indexes"] == [2]


def test_history_parent_normalizer_attaches_custom_loop_to_base_experiment() -> None:
    rows = [
        {
            "experiment_id": "qe_20260429_015755_c4ba_base",
            "parent_experiment_id": None,
            "qe_task_id": "qe_20260429_015755_c4ba",
            "is_evolution_loop": False,
        },
        {
            "experiment_id": "qe_20260429_015755_c4ba_L1",
            "parent_experiment_id": "qe_20260429_015755_c4ba",
            "qe_task_id": "qe_20260429_015755_c4ba",
            "is_evolution_loop": True,
            "_evolution_base_experiment_id": "qe_20260429_015755_c4ba_base",
            "_evolution_task_type": "custom_evo",
        },
    ]

    normalized = ConfigComposer._normalize_history_parent_ids(
        rows,
        {"qe_20260429_015755_c4ba_base"},
    )

    assert normalized[1]["parent_experiment_id"] == "qe_20260429_015755_c4ba_base"
    assert "_evolution_base_experiment_id" not in normalized[1]


def test_history_parent_normalizer_keeps_standard_auto_evolution_parent() -> None:
    rows = [
        {
            "experiment_id": "qe_20260416_082012_L2",
            "parent_experiment_id": "qe_20260416_082012",
            "qe_task_id": "qe_20260416_082012",
            "is_evolution_loop": True,
            "_evolution_base_experiment_id": "qe_20260416_082012",
        },
    ]

    normalized = ConfigComposer._normalize_history_parent_ids(
        rows,
        {"qe_20260416_082012"},
    )

    assert normalized[0]["parent_experiment_id"] == "qe_20260416_082012"
