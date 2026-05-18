"""StrategyPackage binding helpers for MiniQMT virtual strategies."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Protocol

from backend.services.selection_center.models import SelectionRunStatus
from backend.services.strategy_package.models import PackageStatus
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError

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
        return self._repository.create_package_binding(binding)
