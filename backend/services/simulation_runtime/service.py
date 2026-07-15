"""Services for broker-neutral runtime releases and simulation bindings."""

from __future__ import annotations

import re
from datetime import date
from typing import Any

from backend.services.strategy_package.models import PackageStatus
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


def _optional_stripped(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _miniqmt_account_group_id(broker_account_id: str | None) -> str | None:
    account_id = _optional_stripped(broker_account_id)
    if account_id is None:
        return None
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", account_id).strip("_") or "unassigned"
    return f"ag_minqmt_{safe}_sim"


class StrategyRuntimeReleaseService:
    def __init__(
        self,
        repository: SimulationRuntimeRepository | InMemorySimulationRuntimeRepository | Any | None = None,
        *,
        package_lifecycle_reader: Any | None = None,
    ) -> None:
        self.repository = repository or SimulationRuntimeRepository()
        if package_lifecycle_reader is None and isinstance(self.repository, SimulationRuntimeRepository):
            from backend.services.strategy_package.repository import StrategyPackageRepository

            package_lifecycle_reader = StrategyPackageRepository()
        self.package_lifecycle_reader = package_lifecycle_reader

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
        execution_policy_json: dict[str, Any] | None = None,
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
        if execution_policy_json is not None:
            if not isinstance(execution_policy_json, dict) or not execution_policy_json:
                raise RuntimeConfigInvalidError(
                    "StrategyRuntimeRelease execution_policy_json must be a non-empty object when provided",
                    context={"package_id": package_id, "execution_policy_version_id": execution_policy_version_id},
                )
            release_config["execution_policy"]["policy_json"] = dict(execution_policy_json)
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
        account_group_id: str | None = None,
        strategy_slot_id: str | None = None,
        strategy_name: str | None = None,
        order_remark_prefix: str | None = None,
        approval_state: SimulationBindingApprovalState = SimulationBindingApprovalState.DRAFT,
        binding_metadata: dict[str, Any] | None = None,
        miniqmt_quote_control: dict[str, Any] | None = None,
        effective_from: date | None = None,
        effective_to: date | None = None,
        created_by: str | None = None,
        created_reason: str | None = None,
    ) -> SimulationReleaseBinding:
        self._require_package_lifecycle_allows_new_binding(release)
        metadata = dict(binding_metadata or {})
        backend = broker_backend if isinstance(broker_backend, SimulationBrokerBackend) else SimulationBrokerBackend(str(broker_backend))
        normalized_account_group_id = _optional_stripped(account_group_id)
        normalized_strategy_slot_id = _optional_stripped(strategy_slot_id)
        if backend == SimulationBrokerBackend.MINIQMT_SIM:
            normalized_account_group_id = normalized_account_group_id or _miniqmt_account_group_id(broker_account_id)
            normalized_strategy_slot_id = normalized_strategy_slot_id or _optional_stripped(strategy_name) or str(strategy_id).strip()
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
        if backend == SimulationBrokerBackend.MINIQMT_SIM:
            from backend.services.miniqmt_execution_runtime.b0_quote_v2 import QuoteControlBindingV1
            from backend.execution_algos.adaptive_is.contracts import ControlRevision

            if miniqmt_quote_control is None:
                raise RuntimeConfigInvalidError(
                    "new MiniQMT SIM bindings require an explicit B0_QUOTE_V2 quote-control revision",
                    context={
                        "reason_code": "MINIQMT_B0_QUOTE_V2_BINDING_REQUIRED",
                        "broker_backend": backend.value,
                        "strategy_id": strategy_id,
                        "legacy_fallback": False,
                    },
                )
            parsed_quote_control = QuoteControlBindingV1.from_binding_config(
                {"miniqmt_quote_control": miniqmt_quote_control}
            )
            if parsed_quote_control.control_revision is not ControlRevision.B0_QUOTE_V2:
                raise RuntimeConfigInvalidError(
                    "new MiniQMT SIM bindings cannot select LEGACY_B0",
                    context={
                        "reason_code": "MINIQMT_B0_QUOTE_V2_BINDING_REQUIRED",
                        "broker_backend": backend.value,
                        "strategy_id": strategy_id,
                        "control_revision": parsed_quote_control.control_revision.value,
                        "required_control_revision": ControlRevision.B0_QUOTE_V2.value,
                        "legacy_fallback": False,
                    },
                )
            binding_config["miniqmt_quote_control"] = parsed_quote_control.canonical_payload()
        elif miniqmt_quote_control is not None:
            raise RuntimeConfigInvalidError(
                "miniqmt_quote_control is valid only for MiniQMT SIM bindings",
                context={"broker_backend": backend.value, "strategy_id": strategy_id},
            )
        if normalized_account_group_id is not None:
            binding_config["account_group_id"] = normalized_account_group_id
        if normalized_strategy_slot_id is not None:
            binding_config["strategy_slot_id"] = normalized_strategy_slot_id
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
            account_group_id=normalized_account_group_id,
            strategy_slot_id=normalized_strategy_slot_id,
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

    def _require_package_lifecycle_allows_new_binding(self, release: StrategyRuntimeRelease) -> None:
        """Check only package lifecycle at the simulation admission boundary.

        StrategyPackage completeness, model assets, and alpha-mode validation
        belong to the package entry writer and are deliberately not repeated
        here.  The in-memory repository has no authoritative package store; its
        tests can inject ``package_lifecycle_reader`` when lifecycle behavior is
        under test.  Production repositories always receive the canonical
        StrategyPackage reader above.
        """

        reader = self.package_lifecycle_reader
        if reader is None:
            if isinstance(self.repository, InMemorySimulationRuntimeRepository):
                return
            raise RuntimeConfigInvalidError(
                "simulation binding admission requires the authoritative StrategyPackage lifecycle reader",
                context={
                    "reason_code": "SIMULATION_BINDING_PACKAGE_LIFECYCLE_READER_MISSING",
                    "package_id": release.package_id,
                    "release_id": release.release_id,
                },
            )
        get_package = getattr(reader, "get", None)
        if not callable(get_package):
            raise RuntimeConfigInvalidError(
                "simulation binding admission StrategyPackage lifecycle reader is invalid",
                context={
                    "reason_code": "SIMULATION_BINDING_PACKAGE_LIFECYCLE_READER_INVALID",
                    "package_id": release.package_id,
                    "release_id": release.release_id,
                },
            )
        record = get_package(release.package_id)
        record_package_id = str(getattr(record, "package_id", "") or "").strip()
        raw_status = getattr(record, "package_status", None)
        status_value = str(getattr(raw_status, "value", raw_status) or "").strip()
        if record_package_id != release.package_id or not status_value:
            raise RuntimeConfigInvalidError(
                "simulation binding admission StrategyPackage lifecycle identity is incomplete",
                context={
                    "reason_code": "SIMULATION_BINDING_PACKAGE_LIFECYCLE_INVALID",
                    "package_id": release.package_id,
                    "release_id": release.release_id,
                    "record_package_id": record_package_id or None,
                    "package_status": status_value or None,
                },
            )
        try:
            package_status = PackageStatus(status_value)
        except ValueError as exc:
            raise RuntimeConfigInvalidError(
                "simulation binding admission StrategyPackage lifecycle status is invalid",
                context={
                    "reason_code": "SIMULATION_BINDING_PACKAGE_LIFECYCLE_INVALID",
                    "package_id": release.package_id,
                    "release_id": release.release_id,
                    "record_package_id": record_package_id,
                    "package_status": status_value,
                },
            ) from exc
        if package_status == PackageStatus.RETIRED:
            raise RuntimeConfigInvalidError(
                "retired StrategyPackage cannot create a new simulation release binding",
                context={
                    "reason_code": "SIMULATION_BINDING_PACKAGE_RETIRED",
                    "package_id": release.package_id,
                    "release_id": release.release_id,
                    "package_status": PackageStatus.RETIRED.value,
                    "strategy_package_revalidation_performed": False,
                },
            )

    @staticmethod
    def daily_strategy_profile_version_id_from_runtime_config(runtime_config: dict[str, Any] | None) -> str:
        profile = (runtime_config or {}).get("runtime_profile") or {}
        selection = profile.get("selection") if isinstance(profile, dict) else {}
        raw = selection.get("daily_strategy_id") if isinstance(selection, dict) else None
        return str(raw or DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID).strip()
