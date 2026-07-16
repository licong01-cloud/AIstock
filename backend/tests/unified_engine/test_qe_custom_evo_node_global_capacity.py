import inspect

import pytest

from backend.services.quantevolver.node_execution import (
    QENodePreflightError,
    normalize_node_parallelism,
    resolve_qe_node_global_loop_limit,
    resolve_qe_node_global_loop_limits,
)
from backend.services.quantevolver.qe_evolution_service import AutoEvolutionScheduler


def _custom_task(task_id: str, *, node_id: str = "wsl2-5080", limit: int = 2):
    return {
        "task_id": task_id,
        "task_type": "custom_evo",
        "node_id": node_id,
        "strategy_evo_config": {
            "engine_mode": "unified",
            "node_parallelism": {node_id: limit},
            "loops": [
                {
                    "loop_index": 1,
                    "node_id": node_id,
                    "factor_keys": ["Alpha001"],
                    "model_id": "model_a",
                },
                {
                    "loop_index": 2,
                    "node_id": node_id,
                    "factor_keys": ["Alpha002"],
                    "model_id": "model_a",
                },
            ],
        },
    }


class _GlobalCapacityCursor:
    def __init__(self, *, node_active_count: int, task_active_count: int):
        self.node_active_count = node_active_count
        self.task_active_count = task_active_count
        self.sql: list[str] = []
        self.params: list[object] = []

    def execute(self, sql, params=None):
        self.sql.append(" ".join(str(sql).split()))
        self.params.append(params)

    def fetchone(self):
        return {
            "active_count": self.node_active_count,
            "task_active_count": self.task_active_count,
        }


def _enforce(*, task_id: str, node_active_count: int, task_active_count: int, limit: int = 2):
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    cursor = _GlobalCapacityCursor(
        node_active_count=node_active_count,
        task_active_count=task_active_count,
    )
    slot = scheduler._enforce_custom_evo_node_parallelism_slot(
        cursor,
        task=_custom_task(task_id, limit=limit),
        loop_index=2,
        target_node_id="wsl2-5080",
        loop_db_id=f"{task_id}_Loop2",
    )
    return slot, cursor


def test_approved_node_global_loop_limits_are_explicit(monkeypatch):
    monkeypatch.delenv("AISTOCK_QE_NODE_GLOBAL_LOOP_LIMITS_JSON", raising=False)

    assert resolve_qe_node_global_loop_limits() == {
        "wsl2-5080": 2,
        "rdagent-node1": 4,
    }
    assert resolve_qe_node_global_loop_limit("wsl2-5080") == 2
    assert resolve_qe_node_global_loop_limit("rdagent-node1") == 4


def test_node_parallelism_cannot_exceed_node_global_capacity(monkeypatch):
    monkeypatch.delenv("AISTOCK_QE_NODE_GLOBAL_LOOP_LIMITS_JSON", raising=False)

    assert normalize_node_parallelism({"wsl2-5080"}, {"wsl2-5080": 2}) == {
        "wsl2-5080": 2
    }
    with pytest.raises(QENodePreflightError) as exc_info:
        normalize_node_parallelism({"wsl2-5080"}, {"wsl2-5080": 3})

    assert exc_info.value.error_code == "QE_NODE_PARALLELISM_OUT_OF_RANGE"
    assert exc_info.value.context == {"node_id": "wsl2-5080", "value": 3, "max": 2}


def test_node_global_limit_override_is_strict_and_not_silently_ignored(monkeypatch):
    monkeypatch.setenv(
        "AISTOCK_QE_NODE_GLOBAL_LOOP_LIMITS_JSON",
        '{"wsl2-5080": 1, "rdagent-node1": 3, "new-node": 2}',
    )
    assert resolve_qe_node_global_loop_limits() == {
        "wsl2-5080": 1,
        "rdagent-node1": 3,
        "new-node": 2,
    }

    monkeypatch.setenv("AISTOCK_QE_NODE_GLOBAL_LOOP_LIMITS_JSON", "not-json")
    with pytest.raises(QENodePreflightError) as exc_info:
        resolve_qe_node_global_loop_limits()
    assert exc_info.value.error_code == "QE_NODE_GLOBAL_LOOP_LIMITS_JSON_INVALID"

    monkeypatch.setenv("AISTOCK_QE_NODE_GLOBAL_LOOP_LIMITS_JSON", '{"wsl2-5080": 1.5}')
    with pytest.raises(QENodePreflightError) as exc_info:
        resolve_qe_node_global_loop_limits()
    assert exc_info.value.error_code == "QE_NODE_GLOBAL_LOOP_LIMIT_INVALID"


def test_cross_task_active_loops_exhaust_wsl_node_global_capacity(monkeypatch):
    monkeypatch.delenv("AISTOCK_QE_NODE_GLOBAL_LOOP_LIMITS_JSON", raising=False)
    slot, cursor = _enforce(
        task_id="qe_new_task",
        node_active_count=2,
        task_active_count=0,
        limit=2,
    )

    assert slot["available"] is False
    assert slot["active_count"] == 2
    assert slot["task_active_count"] == 0
    assert slot["global_limit"] == 2
    assert slot["unavailable_reasons"] == ["node_global_loop_limit_reached"]
    assert cursor.params[0] == ("qe_node_global_parallelism:wsl2-5080",)
    assert "FROM qe_evolution_loops loops JOIN qe_evolution_tasks tasks" in cursor.sql[1]
    assert "COALESCE(NULLIF(loops.node_id, ''), NULLIF(tasks.node_id, ''), %s) = %s" in cursor.sql[1]
    assert cursor.params[1][-1] == ["running", "processing"]


def test_task_local_limit_is_preserved_inside_node_global_capacity(monkeypatch):
    monkeypatch.delenv("AISTOCK_QE_NODE_GLOBAL_LOOP_LIMITS_JSON", raising=False)
    slot, _cursor = _enforce(
        task_id="qe_single_slot_task",
        node_active_count=1,
        task_active_count=1,
        limit=1,
    )

    assert slot["available"] is False
    assert slot["global_limit"] == 2
    assert slot["unavailable_reasons"] == ["task_parallelism_limit_reached"]


def test_slot_is_available_only_below_both_task_and_node_limits(monkeypatch):
    monkeypatch.delenv("AISTOCK_QE_NODE_GLOBAL_LOOP_LIMITS_JSON", raising=False)
    slot, _cursor = _enforce(
        task_id="qe_available_task",
        node_active_count=1,
        task_active_count=0,
        limit=2,
    )

    assert slot["available"] is True
    assert slot["unavailable_reasons"] == []


def test_different_tasks_share_the_same_postgres_node_lock(monkeypatch):
    monkeypatch.delenv("AISTOCK_QE_NODE_GLOBAL_LOOP_LIMITS_JSON", raising=False)
    _slot_a, cursor_a = _enforce(
        task_id="qe_task_a",
        node_active_count=0,
        task_active_count=0,
    )
    _slot_b, cursor_b = _enforce(
        task_id="qe_task_b",
        node_active_count=0,
        task_active_count=0,
    )

    assert cursor_a.params[0] == cursor_b.params[0]
    assert cursor_a.params[0] == ("qe_node_global_parallelism:wsl2-5080",)


def test_non_custom_evolution_tasks_share_the_same_node_capacity(monkeypatch):
    monkeypatch.delenv("AISTOCK_QE_NODE_GLOBAL_LOOP_LIMITS_JSON", raising=False)
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    cursor = _GlobalCapacityCursor(node_active_count=2, task_active_count=0)
    task = {
        "task_id": "qe_auto_task",
        "task_type": "auto_evo",
        "node_id": "wsl2-5080",
    }

    slot = scheduler._enforce_custom_evo_node_parallelism_slot(
        cursor,
        task=task,
        loop_index=1,
        target_node_id="wsl2-5080",
        loop_db_id="qe_auto_task_Loop1",
    )

    assert slot["limit"] == 1
    assert slot["global_limit"] == 2
    assert slot["available"] is False
    assert slot["unavailable_reasons"] == ["node_global_loop_limit_reached"]


@pytest.mark.parametrize(
    "method_name,submit_marker",
    [
        ("submit_next_loop", "await executor.submit"),
        ("submit_strategy_evo_loop", "await executor.submit"),
        ("retry_loop", "result = await executor.submit"),
        ("_submit_multi_alpha_loop", "result = engine.run()"),
    ],
)
def test_all_single_node_evolution_submit_paths_acquire_capacity_before_submit(
    method_name,
    submit_marker,
):
    source = inspect.getsource(getattr(AutoEvolutionScheduler, method_name))

    capacity_index = source.index("self._mark_custom_evo_loop_running_when_slot_available")
    submit_index = source.index(submit_marker)
    assert capacity_index < submit_index
