from backend.services.quantevolver.qe_evolution_service import derive_custom_evo_final_status


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
