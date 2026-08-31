"""Internal-only LocalSIM account, release, binding, and lineage control plane."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any, Callable

from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError

from .models import (
    RuntimeReleaseValidationState,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
    SimulationReleaseBinding,
    StrategyRuntimeRelease,
    canonical_json_sha256,
)
from .repository import InMemorySimulationRuntimeRepository
from .service import StrategyRuntimeReleaseService
from .successor_models import (
    LEGACY_LOCALSIM_LINEAGE_SCHEMA,
    SIMULATION_ACCOUNT_SCHEMA,
    LegacyLocalSimAccountInventoryV1,
    LegacyLocalSimAccountLineageV1,
    LegacyLocalSimLineageStatus,
    LocalSimSafeBoundaryDecisionV1,
    SimulationAccountStatus,
    SimulationAccountV1,
)
from .successor_repository import LocalSimSuccessorRepositoryProtocol


Clock = Callable[[], datetime]


def _utc_now() -> datetime:
    return datetime.now(UTC)


class LocalSimControlPlaneService:
    """Command/query authority not registered as a product route until SIM-LR-C."""

    def __init__(
        self,
        *,
        repository: LocalSimSuccessorRepositoryProtocol,
        package_lifecycle_reader: Any | None = None,
        clock: Clock = _utc_now,
    ) -> None:
        self.repository = repository
        self.package_lifecycle_reader = package_lifecycle_reader
        self.clock = clock

    def create_account(
        self,
        *,
        account_name: str,
        package_id: str,
        manifest_sha256: str,
        admission_receipt_id: str,
        initial_capital: float,
        runtime_profile_id: str,
        runtime_profile_version_id: str,
        runtime_profile_sha256: str,
        daily_strategy_profile_version_id: str,
        execution_policy_version_id: str,
        execution_policy_sha256: str,
        execution_policy_json: dict[str, Any],
        tail_policy_version_id: str,
        tail_policy_sha256: str,
        release_metadata: dict[str, Any] | None = None,
        requested_execution_policy_audit: dict[str, Any] | None = None,
        effective_from: date | None = None,
        effective_to: date | None = None,
        created_by: str,
        created_reason: str | None = None,
    ) -> tuple[SimulationAccountV1, StrategyRuntimeRelease, SimulationReleaseBinding]:
        now = self._now()
        account = self._build_account(
            account_name=account_name,
            package_id=package_id,
            manifest_sha256=manifest_sha256,
            admission_receipt_id=admission_receipt_id,
            initial_capital=initial_capital,
            lineage_source_legacy_account_id=None,
            status=SimulationAccountStatus.ACTIVE,
            created_by=created_by,
            now=now,
        )
        audit = dict(requested_execution_policy_audit or {})
        if audit:
            audit["consulted_for_execution"] = False
        metadata = dict(release_metadata or {})
        if audit:
            metadata["requested_execution_policy_audit"] = audit
        release, binding = self._build_release_binding(
            account=account,
            base_release_id=None,
            runtime_profile_id=runtime_profile_id,
            runtime_profile_version_id=runtime_profile_version_id,
            runtime_profile_sha256=runtime_profile_sha256,
            daily_strategy_profile_version_id=daily_strategy_profile_version_id,
            execution_policy_version_id=execution_policy_version_id,
            execution_policy_sha256=execution_policy_sha256,
            execution_policy_json=execution_policy_json,
            tail_policy_version_id=tail_policy_version_id,
            tail_policy_sha256=tail_policy_sha256,
            release_metadata=metadata,
            effective_from=effective_from,
            effective_to=effective_to,
            created_by=created_by,
            created_reason=created_reason,
        )
        return self.repository.create_account_bundle(account=account, release=release, binding=binding)

    def get_account(self, account_id: str) -> SimulationAccountV1:
        return self.repository.get_account(account_id)

    def create_successor_release(
        self,
        *,
        account_id: str,
        base_release_id: str,
        base_binding_id: str,
        runtime_profile_id: str,
        runtime_profile_version_id: str,
        runtime_profile_sha256: str,
        daily_strategy_profile_version_id: str,
        execution_policy_version_id: str,
        execution_policy_sha256: str,
        execution_policy_json: dict[str, Any],
        tail_policy_version_id: str,
        tail_policy_sha256: str,
        release_metadata: dict[str, Any] | None = None,
        requested_execution_policy_audit: dict[str, Any] | None = None,
        effective_from: date,
        created_by: str,
        created_reason: str | None = None,
    ) -> tuple[StrategyRuntimeRelease, SimulationReleaseBinding]:
        account = self.repository.get_account(account_id)
        if account.status is SimulationAccountStatus.RETIRED:
            raise InvalidStateTransitionError(
                "retired LocalSIM account cannot receive a successor release",
                context={"reason_code": "LOCALSIM_SUCCESSOR_ACCOUNT_RETIRED", "account_id": account_id},
            )
        base_release = self.repository.get_release(base_release_id)
        base_binding = self.repository.get_binding(base_binding_id)
        if base_release.package_id != account.package_id or base_release.manifest_sha256 != account.manifest_sha256:
            raise InvalidStateTransitionError(
                "LocalSIM successor base release does not match account alpha core",
                context={"reason_code": "LOCALSIM_SUCCESSOR_BASE_RELEASE_MISMATCH"},
            )
        if base_binding.release_id != base_release.release_id or base_binding.binding_hash is None:
            raise InvalidStateTransitionError(
                "LocalSIM successor base binding does not match the base release",
                context={"reason_code": "LOCALSIM_SUCCESSOR_BASE_BINDING_MISMATCH"},
            )
        audit = dict(requested_execution_policy_audit or {})
        if audit:
            audit["consulted_for_execution"] = False
        metadata = dict(release_metadata or {})
        if audit:
            metadata["requested_execution_policy_audit"] = audit
        release, binding = self._build_release_binding(
            account=account,
            base_release_id=base_release.release_id,
            runtime_profile_id=runtime_profile_id,
            runtime_profile_version_id=runtime_profile_version_id,
            runtime_profile_sha256=runtime_profile_sha256,
            daily_strategy_profile_version_id=daily_strategy_profile_version_id,
            execution_policy_version_id=execution_policy_version_id,
            execution_policy_sha256=execution_policy_sha256,
            execution_policy_json=execution_policy_json,
            tail_policy_version_id=tail_policy_version_id,
            tail_policy_sha256=tail_policy_sha256,
            release_metadata=metadata,
            effective_from=effective_from,
            effective_to=None,
            created_by=created_by,
            created_reason=created_reason,
        )
        return self.repository.create_successor_binding(
            account=account,
            source_binding_id=base_binding.binding_id,
            expected_source_binding_hash=base_binding.binding_hash,
            source_effective_to=effective_from - timedelta(days=1),
            release=release,
            binding=binding,
        )

    def pause_account(self, *, account_id: str, expected_version: int) -> SimulationAccountV1:
        return self._transition_account(
            account_id=account_id,
            expected_version=expected_version,
            allowed_from={SimulationAccountStatus.ACTIVE},
            target_status=SimulationAccountStatus.PAUSED,
        )

    def resume_account(self, *, account_id: str, expected_version: int) -> SimulationAccountV1:
        return self._transition_account(
            account_id=account_id,
            expected_version=expected_version,
            allowed_from={SimulationAccountStatus.PAUSED},
            target_status=SimulationAccountStatus.ACTIVE,
        )

    def retire_account(self, *, account_id: str, expected_version: int) -> SimulationAccountV1:
        return self._transition_account(
            account_id=account_id,
            expected_version=expected_version,
            allowed_from={SimulationAccountStatus.ACTIVE, SimulationAccountStatus.PAUSED},
            target_status=SimulationAccountStatus.RETIRED,
        )

    def prepare_legacy_lineage(
        self,
        inventory: LegacyLocalSimAccountInventoryV1,
        *,
        created_by: str,
    ) -> tuple[SimulationAccountV1, LegacyLocalSimAccountLineageV1]:
        if not inventory.runtime_owned or not inventory.retained_by_user:
            raise InvalidStateTransitionError(
                "legacy LocalSIM account is not eligible for successor lineage",
                context={
                    "reason_code": "LOCALSIM_LINEAGE_INVENTORY_NOT_RETAINED",
                    "legacy_account_id": inventory.legacy_account_id,
                },
            )
        if inventory.current_status is SimulationAccountStatus.RETIRED:
            raise InvalidStateTransitionError(
                "retired legacy LocalSIM account cannot be reactivated",
                context={"reason_code": "LOCALSIM_LINEAGE_TERMINAL_ACCOUNT"},
            )
        if inventory.in_flight_economic_transactions != 0:
            raise InvalidStateTransitionError(
                "legacy LocalSIM lineage requires zero in-flight economic transactions",
                context={
                    "reason_code": "LOCALSIM_LINEAGE_IN_FLIGHT_ECONOMIC_TRANSACTION",
                    "count": inventory.in_flight_economic_transactions,
                },
            )
        release = self.repository.get_release(inventory.release_id)
        binding = self.repository.get_binding(inventory.binding_id)
        metadata = binding.binding_config_json.get("metadata")
        if (
            release.release_hash != inventory.release_hash
            or binding.binding_hash != inventory.binding_hash
            or binding.release_id != release.release_id
            or release.package_id != inventory.package_id
            or release.manifest_sha256 != inventory.manifest_sha256
            or binding.package_id != inventory.package_id
            or binding.manifest_sha256 != inventory.manifest_sha256
            or binding.broker_backend is not SimulationBrokerBackend.LOCAL_SIM
            or float(binding.capital_allocation) != float(inventory.initial_capital)
            or binding.broker_account_id not in {inventory.legacy_account_id, inventory.ledger_scope_id}
            or not isinstance(metadata, dict)
            or metadata.get("localsim_account_id") is not None
        ):
            raise InvalidStateTransitionError(
                "legacy LocalSIM lineage inventory does not match release, binding, or ledger authority",
                context={"reason_code": "LOCALSIM_LINEAGE_AUTHORITY_MISMATCH"},
            )
        existing = self.repository.get_lineage_by_legacy_account(inventory.legacy_account_id)
        if existing is not None:
            if (
                existing.release_id != inventory.release_id
                or existing.binding_id != inventory.binding_id
                or existing.ledger_scope_id != inventory.ledger_scope_id
                or existing.economic_facts_sha256 != inventory.economic_facts_sha256
            ):
                raise InvalidStateTransitionError(
                    "legacy LocalSIM account already has a different durable lineage",
                    context={"reason_code": "LOCALSIM_LINEAGE_REPLAY_CONFLICT"},
                )
            return self.repository.get_account(existing.account_id), existing

        now = self._now()
        account = self._build_account(
            account_name=inventory.account_name,
            package_id=inventory.package_id,
            manifest_sha256=inventory.manifest_sha256,
            admission_receipt_id=inventory.admission_receipt_id,
            initial_capital=inventory.initial_capital,
            lineage_source_legacy_account_id=inventory.legacy_account_id,
            status=inventory.current_status,
            created_by=created_by,
            now=now,
        )
        lineage_payload = {
            "schema_version": LEGACY_LOCALSIM_LINEAGE_SCHEMA,
            "legacy_account_id": inventory.legacy_account_id,
            "account_id": account.account_id,
            "release_id": inventory.release_id,
            "binding_id": inventory.binding_id,
            "ledger_scope_id": inventory.ledger_scope_id,
            "economic_facts_sha256": inventory.economic_facts_sha256,
        }
        lineage_hash = canonical_json_sha256(lineage_payload)
        lineage = LegacyLocalSimAccountLineageV1(
            lineage_id=f"lslineage_{lineage_hash[:16]}",
            lineage_hash=lineage_hash,
            legacy_account_id=inventory.legacy_account_id,
            account_id=account.account_id,
            release_id=inventory.release_id,
            binding_id=inventory.binding_id,
            ledger_scope_id=inventory.ledger_scope_id,
            economic_facts_sha256=inventory.economic_facts_sha256,
            created_by=created_by,
            created_at=now,
            updated_at=now,
        )
        return self.repository.create_lineage_bundle(account=account, lineage=lineage)

    def mark_lineage_activation_pending(
        self,
        *,
        legacy_account_id: str,
        expected_version: int,
        current_economic_facts_sha256: str,
    ) -> LegacyLocalSimAccountLineageV1:
        lineage = self.repository.get_lineage_by_legacy_account(legacy_account_id)
        if lineage is None:
            raise DataUnavailableError(
                "legacy LocalSIM lineage does not exist",
                context={"legacy_account_id": legacy_account_id},
            )
        if (
            lineage.version != expected_version
            or lineage.status is not LegacyLocalSimLineageStatus.PREPARED
            or lineage.economic_facts_sha256 != current_economic_facts_sha256
        ):
            raise InvalidStateTransitionError(
                "legacy LocalSIM lineage cannot enter safe-boundary pending state",
                context={"reason_code": "LOCALSIM_LINEAGE_PENDING_STATE_INVALID"},
            )
        return self.repository.transition_lineage(
            lineage_id=lineage.lineage_id,
            expected_version=expected_version,
            target_status=LegacyLocalSimLineageStatus.ACTIVATION_PENDING_SAFE_BOUNDARY,
            updated_at=self._now(),
        )

    def activate_legacy_lineage(
        self,
        *,
        legacy_account_id: str,
        expected_version: int,
        current_economic_facts_sha256: str,
        decision: LocalSimSafeBoundaryDecisionV1,
    ) -> LegacyLocalSimAccountLineageV1:
        lineage = self.repository.get_lineage_by_legacy_account(legacy_account_id)
        if lineage is None:
            raise DataUnavailableError(
                "legacy LocalSIM lineage does not exist",
                context={"legacy_account_id": legacy_account_id},
            )
        account = self.repository.get_account(lineage.account_id)
        if (
            lineage.version != expected_version
            or lineage.status is not LegacyLocalSimLineageStatus.ACTIVATION_PENDING_SAFE_BOUNDARY
            or lineage.economic_facts_sha256 != current_economic_facts_sha256
            or account.status is SimulationAccountStatus.RETIRED
            or not decision.eligible
        ):
            raise InvalidStateTransitionError(
                "legacy LocalSIM lineage activation is not at a durable safe boundary",
                context={"reason_code": "LOCALSIM_LINEAGE_ACTIVATION_BOUNDARY_INVALID"},
            )
        return self.repository.transition_lineage(
            lineage_id=lineage.lineage_id,
            expected_version=expected_version,
            target_status=LegacyLocalSimLineageStatus.ACTIVE,
            updated_at=self._now(),
        )

    def _transition_account(
        self,
        *,
        account_id: str,
        expected_version: int,
        allowed_from: set[SimulationAccountStatus],
        target_status: SimulationAccountStatus,
    ) -> SimulationAccountV1:
        current = self.repository.get_account(account_id)
        if current.version != expected_version:
            raise InvalidStateTransitionError(
                "LocalSIM account lifecycle CAS failed",
                context={
                    "reason_code": "LOCALSIM_ACCOUNT_CAS_CONFLICT",
                    "account_id": account_id,
                    "expected_version": expected_version,
                    "actual_version": current.version,
                },
            )
        if current.status not in allowed_from:
            raise InvalidStateTransitionError(
                "LocalSIM account lifecycle transition is invalid",
                context={
                    "reason_code": "LOCALSIM_ACCOUNT_TRANSITION_INVALID",
                    "account_id": account_id,
                    "current_status": current.status.value,
                    "target_status": target_status.value,
                },
            )
        return self.repository.transition_account(
            account_id=account_id,
            expected_version=expected_version,
            target_status=target_status,
            updated_at=self._now(),
        )

    def _build_release_binding(
        self,
        *,
        account: SimulationAccountV1,
        base_release_id: str | None,
        runtime_profile_id: str,
        runtime_profile_version_id: str,
        runtime_profile_sha256: str,
        daily_strategy_profile_version_id: str,
        execution_policy_version_id: str,
        execution_policy_sha256: str,
        execution_policy_json: dict[str, Any],
        tail_policy_version_id: str,
        tail_policy_sha256: str,
        release_metadata: dict[str, Any],
        effective_from: date | None,
        effective_to: date | None,
        created_by: str,
        created_reason: str | None,
    ) -> tuple[StrategyRuntimeRelease, SimulationReleaseBinding]:
        staging = InMemorySimulationRuntimeRepository()
        service = StrategyRuntimeReleaseService(
            repository=staging,
            package_lifecycle_reader=self.package_lifecycle_reader,
        )
        release = service.create_release(
            package_id=account.package_id,
            manifest_sha256=account.manifest_sha256,
            runtime_profile_id=runtime_profile_id,
            runtime_profile_version_id=runtime_profile_version_id,
            runtime_profile_sha256=runtime_profile_sha256,
            daily_strategy_profile_version_id=daily_strategy_profile_version_id,
            execution_policy_version_id=execution_policy_version_id,
            execution_policy_sha256=execution_policy_sha256,
            tail_policy_version_id=tail_policy_version_id,
            tail_policy_sha256=tail_policy_sha256,
            execution_policy_json=execution_policy_json,
            base_release_id=base_release_id,
            validation_state=RuntimeReleaseValidationState.SIM_VALIDATING,
            release_metadata=release_metadata,
            effective_from=effective_from,
            effective_to=effective_to,
            created_by=created_by,
            created_reason=created_reason,
        )
        binding = service.create_binding(
            strategy_id=account.account_id,
            release=release,
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            broker_account_id=account.account_id,
            account_group_id=account.account_id,
            capital_allocation=account.initial_capital,
            approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
            binding_metadata={
                "localsim_account_id": account.account_id,
                "account_schema_version": SIMULATION_ACCOUNT_SCHEMA,
            },
            effective_from=effective_from,
            effective_to=effective_to,
            created_by=created_by,
            created_reason=created_reason,
        )
        return release, binding

    @staticmethod
    def _build_account(
        *,
        account_name: str,
        package_id: str,
        manifest_sha256: str,
        admission_receipt_id: str,
        initial_capital: float,
        lineage_source_legacy_account_id: str | None,
        status: SimulationAccountStatus,
        created_by: str,
        now: datetime,
    ) -> SimulationAccountV1:
        account_config = {
            "schema_version": SIMULATION_ACCOUNT_SCHEMA,
            "account_name": str(account_name).strip(),
            "broker_backend": SimulationBrokerBackend.LOCAL_SIM.value,
            "package_id": str(package_id).strip(),
            "manifest_sha256": str(manifest_sha256).strip(),
            "admission_receipt_id": str(admission_receipt_id).strip(),
            "initial_capital": float(initial_capital),
        }
        if lineage_source_legacy_account_id is not None:
            account_config["lineage_source_legacy_account_id"] = str(lineage_source_legacy_account_id).strip()
        account_hash = canonical_json_sha256(account_config)
        return SimulationAccountV1(
            account_id=f"simacct_{account_hash[:16]}",
            account_hash=account_hash,
            account_name=account_config["account_name"],
            package_id=account_config["package_id"],
            manifest_sha256=account_config["manifest_sha256"],
            admission_receipt_id=account_config["admission_receipt_id"],
            initial_capital=account_config["initial_capital"],
            lineage_source_legacy_account_id=lineage_source_legacy_account_id,
            account_config_json=account_config,
            status=status,
            created_by=created_by,
            created_at=now,
            updated_at=now,
        )

    def _now(self) -> datetime:
        value = self.clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise DataUnavailableError(
                "LocalSIM control-plane clock must be timezone-aware",
                context={"reason_code": "LOCALSIM_CONTROL_CLOCK_INVALID"},
            )
        return value.astimezone(UTC)
