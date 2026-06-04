from __future__ import annotations

import pytest

from backend.services.simulation_runtime import (
    DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
    InMemorySimulationRuntimeRepository,
    StrategyRuntimeReleaseService,
)
from backend.services.simulation_runtime.models import StrategyRuntimeRelease
from backend.services.trading_core.errors import RuntimeConfigInvalidError


def _service() -> StrategyRuntimeReleaseService:
    return StrategyRuntimeReleaseService(repository=InMemorySimulationRuntimeRepository())


def _release_kwargs(**overrides):
    payload = {
        "package_id": "pkg_unit",
        "manifest_sha256": "manifest_hash_unit",
        "runtime_profile_id": "runtime_profile_unit",
        "runtime_profile_version_id": "runtime_profile_version_unit",
        "runtime_profile_sha256": "runtime_profile_hash_unit",
        "daily_strategy_profile_version_id": DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        "execution_policy_version_id": "exec_policy_unit",
        "execution_policy_sha256": "exec_policy_hash_unit",
        "tail_policy_version_id": "tail_policy_unit",
        "tail_policy_sha256": "tail_policy_hash_unit",
        "created_by": "unit_test",
        "created_reason": "baseline runtime release",
    }
    payload.update(overrides)
    return payload


def test_strategy_runtime_release_hash_is_canonical_and_idempotent() -> None:
    service = _service()

    first = service.create_release(**_release_kwargs())
    second = service.create_release(**_release_kwargs())

    assert first.release_id == second.release_id
    assert first.release_hash == second.release_hash
    assert first.release_config_json["schema_version"] == "strategy_runtime_release_v1"
    assert first.release_config_json["package_id"] == "pkg_unit"
    assert "broker_account_id" not in first.release_config_json


def test_strategy_runtime_release_can_persist_full_execution_policy_snapshot() -> None:
    service = _service()
    policy_json = {
        "algo_code": "SNIPER_MINIQMT",
        "algo_config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
    }

    release = service.create_release(**_release_kwargs(execution_policy_json=policy_json))

    assert release.release_config_json["execution_policy"]["policy_json"] == policy_json
    assert release.release_config_json["execution_policy"]["policy_version_id"] == "exec_policy_unit"
    assert release.release_config_json["execution_policy"]["policy_sha256"] == "exec_policy_hash_unit"


def test_strategy_runtime_release_requires_all_policy_versions() -> None:
    service = _service()

    with pytest.raises(RuntimeConfigInvalidError, match="requires all runtime"):
        service.create_release(**_release_kwargs(execution_policy_version_id=""))


def test_strategy_runtime_release_rejects_alpha_core_or_broker_fields() -> None:
    with pytest.raises(RuntimeConfigInvalidError, match="cannot contain alpha-core"):
        StrategyRuntimeRelease(
            release_id="srr_bad_alpha",
            package_id="pkg_unit",
            manifest_sha256="manifest_hash_unit",
            runtime_profile_id="runtime_profile_unit",
            runtime_profile_version_id="runtime_profile_version_unit",
            runtime_profile_sha256="runtime_profile_hash_unit",
            daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
            execution_policy_version_id="exec_policy_unit",
            execution_policy_sha256="exec_policy_hash_unit",
            tail_policy_version_id="tail_policy_unit",
            tail_policy_sha256="tail_policy_hash_unit",
            release_config_json={
                "schema_version": "strategy_runtime_release_v1",
                "package_id": "pkg_unit",
                "factor_set": ["forbidden_factor_override"],
            },
        )

    with pytest.raises(RuntimeConfigInvalidError, match="cannot contain alpha-core"):
        StrategyRuntimeRelease(
            release_id="srr_bad_broker",
            package_id="pkg_unit",
            manifest_sha256="manifest_hash_unit",
            runtime_profile_id="runtime_profile_unit",
            runtime_profile_version_id="runtime_profile_version_unit",
            runtime_profile_sha256="runtime_profile_hash_unit",
            daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
            execution_policy_version_id="exec_policy_unit",
            execution_policy_sha256="exec_policy_hash_unit",
            tail_policy_version_id="tail_policy_unit",
            tail_policy_sha256="tail_policy_hash_unit",
            release_config_json={
                "schema_version": "strategy_runtime_release_v1",
                "package_id": "pkg_unit",
                "broker_account_id": "forbidden_account",
            },
        )


def test_strategy_runtime_release_hash_changes_only_for_policy_changes_not_binding_changes() -> None:
    service = _service()
    baseline = service.create_release(**_release_kwargs())
    changed_execution = service.create_release(
        **_release_kwargs(execution_policy_version_id="exec_policy_v2", execution_policy_sha256="exec_policy_hash_v2")
    )
    binding_a = service.create_binding(
        strategy_id="strategy_a",
        release=baseline,
        broker_backend="minqmt_sim",
        broker_account_id="broker_account_a",
        capital_allocation=10_000_000,
        strategy_name="strategy_a",
        order_remark_prefix="aistock_a",
    )
    binding_b = service.create_binding(
        strategy_id="strategy_a",
        release=baseline,
        broker_backend="minqmt_sim",
        broker_account_id="broker_account_b",
        capital_allocation=20_000_000,
        strategy_name="strategy_a_v2",
        order_remark_prefix="aistock_a_v2",
    )

    assert changed_execution.release_hash != baseline.release_hash
    assert binding_a.binding_hash != binding_b.binding_hash
    assert binding_a.release_hash == baseline.release_hash
    assert binding_b.release_hash == baseline.release_hash
