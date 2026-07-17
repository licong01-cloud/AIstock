from typing import Any

import backend.services.quantevolver.qe_evolution_service as qes
from backend.services.quantevolver.qe_evolution_service import (
    AutoEvolutionScheduler,
    derive_custom_evo_final_status,
)
from backend.services.quantevolver.config_composer import ConfigComposer


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
            assert "COUNT(*) AS count" in self.query
            return [{"status": "completed", "count": 2}]

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
