"""Post-close diagnostics preserve economic state and distinguish durable failures."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from backend.services.trading_core.errors import DataUnavailableError
from backend.services.simulation_runtime.models import SimulationDailyRunStatus
from backend.services.simulation_runtime.scheduler import SimulationLifecycleScheduler


@pytest.mark.parametrize(
    ("persistence_status", "expected_status", "reason_code"),
    [
        ("PERSISTED", SimulationDailyRunStatus.SUCCEEDED, "LOCALSIM_POST_CLOSE_PERSISTED_SUCCESS"),
        ("PERSISTED_WITH_MARKET_RESIDUAL", SimulationDailyRunStatus.SUCCEEDED, "LOCALSIM_POST_CLOSE_PERSISTED_SUCCESS"),
        (
            "PERSISTED_WITH_CAPACITY_RESIDUAL",
            SimulationDailyRunStatus.FAILED_TERMINAL,
            "LOCALSIM_POST_CLOSE_CAPACITY_RESIDUAL_TERMINAL",
        ),
        (
            "PERSISTED_WITH_RESIDUAL",
            SimulationDailyRunStatus.FAILED_TERMINAL,
            "LOCALSIM_POST_CLOSE_EXECUTION_RESIDUAL_TERMINAL",
        ),
        (
            "PERSISTED_WITH_TERMINAL_FAILURE",
            SimulationDailyRunStatus.FAILED_TERMINAL,
            "LOCALSIM_POST_CLOSE_PERSISTED_EXECUTION_FAILURE",
        ),
        ("UNKNOWN", SimulationDailyRunStatus.FAILED_TERMINAL, "LOCALSIM_POST_CLOSE_DURABLE_PERSISTENCE_MISSING"),
    ],
)
def test_post_close_persistence_status_is_accurate_and_does_not_mutate_economics(
    persistence_status, expected_status, reason_code
):
    payload = {
        "broker_called": True,
        "submitted_intents": 50,
        "local_sim_persistence": {"status": persistence_status, "terminal": True},
        "residual_orders": [{"symbol": "688696.SH", "remaining_quantity": 14}],
    }
    original = deepcopy(payload)
    run = SimpleNamespace(run_id="diagnostic-only", run_payload_json=payload)

    result = SimulationLifecycleScheduler._localsim_post_close_terminal_status(run)

    assert result[0] is expected_status
    assert result[2] == reason_code
    assert payload == original


@pytest.mark.parametrize("status", ["PERSISTED", "PERSISTED_WITH_RESIDUAL", "PERSISTED_WITH_TERMINAL_FAILURE"])
def test_nonterminal_generation_retains_authority_over_summary_status(status):
    run = SimpleNamespace(
        run_id="active-generation",
        run_payload_json={"broker_called": True, "local_sim_persistence": {"status": status, "terminal": False}},
    )
    assert SimulationLifecycleScheduler._localsim_post_close_terminal_status(run) == (None, None, None, None)


@pytest.mark.parametrize("terminal", [None, "true", 1])
def test_invalid_terminal_flag_still_fails_closed(terminal):
    run = SimpleNamespace(
        run_id="invalid-generation",
        run_payload_json={"local_sim_persistence": {"status": "PERSISTED_WITH_RESIDUAL", "terminal": terminal}},
    )
    with pytest.raises(DataUnavailableError, match="terminal flag must be a boolean"):
        SimulationLifecycleScheduler._localsim_post_close_terminal_status(run)


def test_absent_persistence_with_broker_side_effect_is_still_missing():
    run = SimpleNamespace(run_id="missing-generation", run_payload_json={"broker_called": True})
    result = SimulationLifecycleScheduler._localsim_post_close_terminal_status(run)
    assert result[0] is SimulationDailyRunStatus.FAILED_TERMINAL
    assert result[2] == "LOCALSIM_POST_CLOSE_DURABLE_PERSISTENCE_MISSING"
