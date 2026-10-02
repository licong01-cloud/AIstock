from __future__ import annotations

import inspect

from backend.services.simulation_execution.localsim import LocalSimBackend
from backend.services.strategy_package.execution_policy import (
    LOCALSIM_TWAP_ONLY_POLICY_VERSION_ID,
    local_sim_twap_only_policy_snapshot,
    validate_frozen_execution_policy_snapshot,
)


def test_localsim_policy_authority_is_one_validated_twap_snapshot() -> None:
    snapshot = local_sim_twap_only_policy_snapshot()

    validated = validate_frozen_execution_policy_snapshot(
        snapshot,
        context={"operation": "test_localsim_twap_policy_authority"},
    )

    assert validated["policy_version_id"] == LOCALSIM_TWAP_ONLY_POLICY_VERSION_ID
    assert validated["policy_json"]["algo_code"] == "TWAP"


def test_execution_owned_runtime_has_no_v25_or_paper_policy_branch() -> None:
    source = inspect.getsource(LocalSimBackend)

    assert "V25_TWO_STAGE" not in source
    assert "V25_1_SMALL_CAP" not in source
    assert "paper_trading_v2" not in source
    assert "manifest_policy_consulted" in source
