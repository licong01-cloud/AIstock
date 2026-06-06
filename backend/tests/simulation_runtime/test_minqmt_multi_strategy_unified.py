from __future__ import annotations

from datetime import date

import pytest

from backend.services.simulation_runtime.models import (
    RuntimeReleaseValidationState,
    SimulationBrokerBackend,
    SimulationReleaseBinding,
    StrategyRuntimeRelease,
)


def _binding(*, strategy_id: str, slot_id: str, strategy_name: str, remark_prefix: str) -> SimulationReleaseBinding:
    config = {
        "schema_version": "simulation_release_binding_v1",
        "strategy_id": strategy_id,
        "release_id": "srr_alpha",
        "release_hash": "hash_alpha",
        "package_id": "pkg_alpha",
        "manifest_sha256": "manifest_hash",
        "broker_backend": "minqmt_sim",
        "broker_account_id": "62266303",
        "account_group_id": "ag_minqmt_62266303_sim",
        "strategy_slot_id": slot_id,
        "capital_allocation": 100000,
        "strategy_name": strategy_name,
        "order_remark_prefix": remark_prefix,
        "approval_state": "SIM_PASSED",
        "metadata": {},
    }
    return SimulationReleaseBinding(
        strategy_id=strategy_id,
        release_id="srr_alpha",
        release_hash="hash_alpha",
        package_id="pkg_alpha",
        manifest_sha256="manifest_hash",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        broker_account_id="62266303",
        account_group_id="ag_minqmt_62266303_sim",
        strategy_slot_id=slot_id,
        capital_allocation=100000,
        strategy_name=strategy_name,
        order_remark_prefix=remark_prefix,
        effective_from=date(2026, 5, 18),
        approval_state="SIM_PASSED",
        binding_config_json=config,
    )


def test_minqmt_simulation_release_bindings_carry_account_group_strategy_slots() -> None:
    slot_a = _binding(strategy_id="strat_a", slot_id="slot_a", strategy_name="UnifiedAlphaA", remark_prefix="ag622-a")
    slot_b = _binding(strategy_id="strat_b", slot_id="slot_b", strategy_name="UnifiedAlphaB", remark_prefix="ag622-b")

    assert {slot_a.account_group_id, slot_b.account_group_id} == {"ag_minqmt_62266303_sim"}
    assert {slot_a.strategy_slot_id, slot_b.strategy_slot_id} == {"slot_a", "slot_b"}
    assert {slot_a.broker_account_id, slot_b.broker_account_id} == {"62266303"}
    assert slot_a.binding_hash != slot_b.binding_hash


def test_strategy_runtime_release_rejects_account_group_fields_in_alpha_core() -> None:
    with pytest.raises(Exception, match="broker-binding fields"):
        StrategyRuntimeRelease(
            package_id="pkg_alpha",
            manifest_sha256="manifest_hash",
            runtime_profile_id="rp",
            runtime_profile_version_id="rp_v1",
            runtime_profile_sha256="rp_hash",
            daily_strategy_profile_version_id="daily_v1",
            execution_policy_version_id="exec_v1",
            execution_policy_sha256="exec_hash",
            tail_policy_version_id="tail_v1",
            tail_policy_sha256="tail_hash",
            release_config_json={"account_group_id": "ag_minqmt_62266303_sim"},
            validation_state=RuntimeReleaseValidationState.SIM_PASSED,
        )
