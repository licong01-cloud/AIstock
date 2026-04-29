from backend.services.quantevolver.qe_evolution_service import derive_custom_evo_final_status
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
