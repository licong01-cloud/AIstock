"""StrategyPackage binding helpers for MiniQMT virtual strategies."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Protocol

from backend.services.selection_center.models import SelectionRunStatus
from backend.services.strategy_package.models import PackageStatus
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError, StrategyPackageValidationError

from .models import BindingStatus, StrategyPackageBinding, new_id


class StrategyPackageReader(Protocol):
    def get(self, package_id: str) -> Any:
        ...


class SelectionRunReader(Protocol):
    def get_run(self, run_id: str) -> Any:
        ...


@dataclass(frozen=True)
class PackageBindingRequest:
    strategy_id: str
    package_id: str
    selection_run_id: str
    trade_date: date | None = None
    target_weight: Decimal | None = None
    top_k: int | None = None
    runtime_config: dict[str, Any] | None = None
    replace_active: bool = False
    replacement_reason: str | None = None


@dataclass(frozen=True)
class PackageBindingResult:
    binding: StrategyPackageBinding
    action: str
    replaced_binding: StrategyPackageBinding | None = None


class QmtStrategyPackageBindingService:
    """Create auditable StrategyPackage bindings for virtual accounts."""

    _ALLOWED_PACKAGE_STATUSES = {
        PackageStatus.SELECTION_ENABLED,
        PackageStatus.PAPER_ENABLED,
        PackageStatus.PAPER_RUNNING,
        PackageStatus.PAPER_PASSED,
    }

    def __init__(
        self,
        *,
        repository: Any,
        package_reader: StrategyPackageReader,
        selection_reader: SelectionRunReader,
    ) -> None:
        self._repository = repository
        self._package_reader = package_reader
        self._selection_reader = selection_reader

    def bind(self, request: PackageBindingRequest) -> StrategyPackageBinding:
        return self.bind_with_result(request).binding

    def bind_with_result(self, request: PackageBindingRequest) -> PackageBindingResult:
        account = self._repository.get_virtual_account(request.strategy_id)
        package_record = self._package_reader.get(request.package_id)
        selection_run = self._selection_reader.get_run(request.selection_run_id)
        if package_record.package_status not in self._ALLOWED_PACKAGE_STATUSES:
            raise StrategyPackageValidationError(
                "strategy package is not enabled for selection or paper usage",
                context={"package_id": request.package_id, "status": package_record.package_status.value},
            )
        if selection_run.status != SelectionRunStatus.SUCCEEDED:
            raise DataUnavailableError(
                "selection run is not succeeded",
                context={"selection_run_id": request.selection_run_id, "status": selection_run.status.value},
            )
        if request.package_id not in selection_run.package_ids:
            raise StrategyPackageValidationError(
                "selection run does not contain the package being bound",
                context={"package_id": request.package_id, "selection_run_id": request.selection_run_id},
            )
        manifest_sha = selection_run.manifest_sha256_by_package.get(request.package_id) or package_record.manifest_sha256
        if manifest_sha != package_record.manifest_sha256:
            raise StrategyPackageValidationError(
                "selection run manifest hash does not match StrategyPackage record",
                context={
                    "package_id": request.package_id,
                    "selection_manifest_sha256": manifest_sha,
                    "package_manifest_sha256": package_record.manifest_sha256,
                },
            )
        binding = StrategyPackageBinding(
            binding_id=new_id("qmtbind"),
            strategy_id=account.strategy_id,
            package_id=request.package_id,
            manifest_sha256=package_record.manifest_sha256,
            selection_run_id=request.selection_run_id,
            trade_date=request.trade_date or selection_run.trade_date,
            target_weight=request.target_weight,
            top_k=request.top_k,
            binding_status=BindingStatus.ACTIVE,
            runtime_config=dict(request.runtime_config or {}),
        )
        active = self._repository.get_active_package_binding(account.strategy_id)
        if active is None:
            return PackageBindingResult(
                binding=self._repository.create_package_binding(binding),
                action="created",
            )
        if _same_binding(active, binding):
            return PackageBindingResult(binding=active, action="idempotent_existing")
        if not request.replace_active:
            raise InvalidStateTransitionError(
                "active package binding already exists; set replace_active=true to roll over",
                context={
                    "strategy_id": account.strategy_id,
                    "active_binding_id": active.binding_id,
                    "active_package_id": active.package_id,
                    "active_selection_run_id": active.selection_run_id,
                    "active_trade_date": active.trade_date.isoformat() if active.trade_date else None,
                    "requested_package_id": binding.package_id,
                    "requested_selection_run_id": binding.selection_run_id,
                    "requested_trade_date": binding.trade_date.isoformat() if binding.trade_date else None,
                },
            )

        reason = (request.replacement_reason or "package_binding_rollover").strip() or "package_binding_rollover"
        binding = replace_runtime_lifecycle(binding, replaces_binding_id=active.binding_id, reason=reason)
        return PackageBindingResult(
            binding=self._repository.replace_active_package_binding(
                binding,
                replaced_binding_id=active.binding_id,
                reason=reason,
            ),
            action="replaced_active",
            replaced_binding=self._repository.get_package_binding(active.binding_id),
        )


def replace_runtime_lifecycle(
    binding: StrategyPackageBinding,
    *,
    replaces_binding_id: str,
    reason: str,
) -> StrategyPackageBinding:
    runtime_config = dict(binding.runtime_config)
    runtime_config["binding_lifecycle"] = {
        **dict(runtime_config.get("binding_lifecycle") or {}),
        "replaces_binding_id": replaces_binding_id,
        "replace_reason": reason,
        "replaced_at": datetime.now(UTC).isoformat(),
    }
    return replace(binding, runtime_config=runtime_config, updated_at=datetime.now(UTC))


def _same_binding(left: StrategyPackageBinding, right: StrategyPackageBinding) -> bool:
    return (
        left.strategy_id == right.strategy_id
        and left.package_id == right.package_id
        and left.manifest_sha256 == right.manifest_sha256
        and left.selection_run_id == right.selection_run_id
        and left.trade_date == right.trade_date
        and left.target_weight == right.target_weight
        and left.top_k == right.top_k
    )
