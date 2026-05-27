"""Services for broker-neutral runtime releases and simulation bindings."""

from __future__ import annotations

from datetime import date
from typing import Any

from backend.services.trading_core.errors import RuntimeConfigInvalidError

from .models import (
    DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
    RuntimeReleaseValidationState,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
    SimulationReleaseBinding,
    StrategyRuntimeRelease,
    assert_binding_payload_boundary,
    assert_release_payload_boundary,
    canonical_json_sha256,
)
from .repository import InMemorySimulationRuntimeRepository, SimulationRuntimeRepository


class StrategyRuntimeReleaseService:
    def __init__(self, repository: SimulationRuntimeRepository | InMemorySimulationRuntimeRepository | Any | None = None) -> None:
        self.repository = repository or SimulationRuntimeRepository()

    def create_release(
        self,
        *,
        package_id: str,
        manifest_sha256: str,
        runtime_profile_id: str,
        runtime_profile_version_id: str,
        runtime_profile_sha256: str,
        daily_strategy_profile_version_id: str,
        execution_policy_version_id: str,
        execution_policy_sha256: str,
        tail_policy_version_id: str,
        tail_policy_sha256: str,
        base_release_id: str | None = None,
        validation_state: RuntimeReleaseValidationState = RuntimeReleaseValidationState.DRAFT,
        validation_evidence: dict[str, Any] | None = None,
        release_metadata: dict[str, Any] | None = None,
        effective_from: date | None = None,
        effective_to: date | None = None,
        created_by: str | None = None,
        created_reason: str | None = None,
    ) -> StrategyRuntimeRelease:
        required = {
            "package_id": package_id,
            "manifest_sha256": manifest_sha256,
            "runtime_profile_id": runtime_profile_id,
            "runtime_profile_version_id": runtime_profile_version_id,
            "runtime_profile_sha256": runtime_profile_sha256,
            "daily_strategy_profile_version_id": daily_strategy_profile_version_id,
            "execution_policy_version_id": execution_policy_version_id,
            "execution_policy_sha256": execution_policy_sha256,
            "tail_policy_version_id": tail_policy_version_id,
            "tail_policy_sha256": tail_policy_sha256,
        }
        missing = [key for key, value in required.items() if not str(value or "").strip()]
        if missing:
            raise RuntimeConfigInvalidError(
                "StrategyRuntimeRelease requires all runtime, daily, execution and tail version identifiers",
                context={"missing_fields": missing},
            )

        evidence = dict(validation_evidence or {})
        metadata = dict(release_metadata or {})
        release_config = {
            "schema_version": "strategy_runtime_release_v1",
            "package_id": str(package_id).strip(),
            "manifest_sha256": str(manifest_sha256).strip(),
            "base_release_id": str(base_release_id).strip() if base_release_id else None,
            "runtime_profile": {
                "profile_id": str(runtime_profile_id).strip(),
                "profile_version_id": str(runtime_profile_version_id).strip(),
                "config_sha256": str(runtime_profile_sha256).strip(),
            },
            "daily_strategy": {
                "profile_version_id": str(daily_strategy_profile_version_id).strip(),
            },
            "execution_policy": {
                "policy_version_id": str(execution_policy_version_id).strip(),
                "policy_sha256": str(execution_policy_sha256).strip(),
            },
            "tail_policy": {
                "policy_version_id": str(tail_policy_version_id).strip(),
                "policy_sha256": str(tail_policy_sha256).strip(),
            },
            "validation_state": validation_state.value,
            "validation_evidence": evidence,
            "metadata": metadata,
        }
        assert_release_payload_boundary(release_config, context={"package_id": package_id})
        release_hash = canonical_json_sha256(release_config)
        release = StrategyRuntimeRelease(
            release_id=f"srr_{release_hash[:16]}",
            package_id=package_id,
            manifest_sha256=manifest_sha256,
            base_release_id=base_release_id,
            runtime_profile_id=runtime_profile_id,
            runtime_profile_version_id=runtime_profile_version_id,
            runtime_profile_sha256=runtime_profile_sha256,
            daily_strategy_profile_version_id=daily_strategy_profile_version_id,
            execution_policy_version_id=execution_policy_version_id,
            execution_policy_sha256=execution_policy_sha256,
            tail_policy_version_id=tail_policy_version_id,
            tail_policy_sha256=tail_policy_sha256,
            release_config_json=release_config,
            release_hash=release_hash,
            validation_state=validation_state,
            validation_evidence=evidence,
            effective_from=effective_from,
            effective_to=effective_to,
            created_by=created_by,
            created_reason=created_reason,
        )
        return self.repository.save_strategy_runtime_release(release)

    def create_binding(
        self,
        *,
        strategy_id: str,
        release: StrategyRuntimeRelease,
        broker_backend: str | SimulationBrokerBackend,
        capital_allocation: float,
        broker_account_id: str | None = None,
        strategy_name: str | None = None,
        order_remark_prefix: str | None = None,
        approval_state: SimulationBindingApprovalState = SimulationBindingApprovalState.DRAFT,
        binding_metadata: dict[str, Any] | None = None,
        effective_from: date | None = None,
        effective_to: date | None = None,
        created_by: str | None = None,
        created_reason: str | None = None,
    ) -> SimulationReleaseBinding:
        metadata = dict(binding_metadata or {})
        backend = broker_backend if isinstance(broker_backend, SimulationBrokerBackend) else SimulationBrokerBackend(str(broker_backend))
        binding_config = {
            "schema_version": "simulation_release_binding_v1",
            "strategy_id": str(strategy_id).strip(),
            "release_id": release.release_id,
            "release_hash": release.release_hash,
            "package_id": release.package_id,
            "manifest_sha256": release.manifest_sha256,
            "broker_backend": backend.value,
            "broker_account_id": str(broker_account_id).strip() if broker_account_id else None,
            "capital_allocation": float(capital_allocation),
            "strategy_name": str(strategy_name).strip() if strategy_name else None,
            "order_remark_prefix": str(order_remark_prefix).strip() if order_remark_prefix else None,
            "approval_state": approval_state.value,
            "metadata": metadata,
        }
        assert_binding_payload_boundary(binding_config, context={"strategy_id": strategy_id})
        binding_hash = canonical_json_sha256(binding_config)
        binding = SimulationReleaseBinding(
            binding_id=f"simbind_{binding_hash[:16]}",
            strategy_id=strategy_id,
            release_id=release.release_id,
            release_hash=release.release_hash or "",
            package_id=release.package_id,
            manifest_sha256=release.manifest_sha256,
            broker_backend=backend,
            broker_account_id=broker_account_id,
            capital_allocation=capital_allocation,
            strategy_name=strategy_name,
            order_remark_prefix=order_remark_prefix,
            approval_state=approval_state,
            binding_config_json=binding_config,
            binding_hash=binding_hash,
            effective_from=effective_from,
            effective_to=effective_to,
            created_by=created_by,
            created_reason=created_reason,
        )
        return self.repository.save_simulation_release_binding(binding)

    @staticmethod
    def daily_strategy_profile_version_id_from_runtime_config(runtime_config: dict[str, Any] | None) -> str:
        profile = (runtime_config or {}).get("runtime_profile") or {}
        selection = profile.get("selection") if isinstance(profile, dict) else {}
        raw = selection.get("daily_strategy_id") if isinstance(selection, dict) else None
        return str(raw or DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID).strip()
