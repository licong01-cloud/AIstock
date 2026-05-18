from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import pytest

from backend.services.qmt_strategy_ledger.models import VirtualAccount, VirtualAccountStatus
from backend.services.qmt_strategy_ledger.package_binding import PackageBindingRequest, QmtStrategyPackageBindingService
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.selection_center.models import SelectionMode, SelectionRun, SelectionRunStatus
from backend.services.strategy_package.models import PackageStatus
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError


ACCOUNT_ID = "62266303"
TRADE_DATE = date(2026, 5, 18)


@dataclass
class FakePackageRecord:
    package_id: str
    package_status: PackageStatus
    manifest_sha256: str


@dataclass
class FakePackageReader:
    record: FakePackageRecord

    def get(self, package_id: str) -> FakePackageRecord:
        assert package_id == self.record.package_id
        return self.record


@dataclass
class FakeSelectionReader:
    run: SelectionRun

    def get_run(self, run_id: str) -> SelectionRun:
        assert run_id == self.run.run_id
        return self.run


def _repo() -> InMemoryQmtStrategyLedgerRepository:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat_a",
            strategy_name="poc_strategy_a",
            display_name="POC Strategy A",
            account_id=ACCOUNT_ID,
            mode="SIM",
            initial_cash=Decimal("10000000"),
            cash=Decimal("10000000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    return repo


def _run(
    *,
    package_ids: list[str] | None = None,
    manifest_sha: str = "sha_a",
    status: SelectionRunStatus = SelectionRunStatus.SUCCEEDED,
) -> SelectionRun:
    return SelectionRun(
        run_id="sel_a",
        mode=SelectionMode.SINGLE_PACKAGE,
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        package_ids=package_ids or ["pkg_a"],
        status=status,
        manifest_sha256_by_package={"pkg_a": manifest_sha},
    )


def test_package_binding_creates_active_binding_with_manifest_evidence() -> None:
    repo = _repo()
    service = QmtStrategyPackageBindingService(
        repository=repo,
        package_reader=FakePackageReader(FakePackageRecord("pkg_a", PackageStatus.SELECTION_ENABLED, "sha_a")),
        selection_reader=FakeSelectionReader(_run()),
    )

    binding = service.bind(
        PackageBindingRequest(
            strategy_id="strat_a",
            package_id="pkg_a",
            selection_run_id="sel_a",
            target_weight=Decimal("0.02"),
            top_k=20,
        )
    )

    assert binding.package_id == "pkg_a"
    assert binding.manifest_sha256 == "sha_a"
    assert binding.trade_date == TRADE_DATE
    assert repo.get_active_package_binding("strat_a") == binding


def test_package_binding_rejects_unavailable_package_selection_and_manifest_mismatch() -> None:
    repo = _repo()

    with pytest.raises(StrategyPackageValidationError, match="not enabled"):
        QmtStrategyPackageBindingService(
            repository=repo,
            package_reader=FakePackageReader(FakePackageRecord("pkg_a", PackageStatus.DRAFT, "sha_a")),
            selection_reader=FakeSelectionReader(_run()),
        ).bind(PackageBindingRequest(strategy_id="strat_a", package_id="pkg_a", selection_run_id="sel_a"))

    with pytest.raises(DataUnavailableError, match="selection run is not succeeded"):
        QmtStrategyPackageBindingService(
            repository=repo,
            package_reader=FakePackageReader(FakePackageRecord("pkg_a", PackageStatus.SELECTION_ENABLED, "sha_a")),
            selection_reader=FakeSelectionReader(_run(status=SelectionRunStatus.FAILED)),
        ).bind(PackageBindingRequest(strategy_id="strat_a", package_id="pkg_a", selection_run_id="sel_a"))

    with pytest.raises(StrategyPackageValidationError, match="manifest hash"):
        QmtStrategyPackageBindingService(
            repository=repo,
            package_reader=FakePackageReader(FakePackageRecord("pkg_a", PackageStatus.SELECTION_ENABLED, "sha_a")),
            selection_reader=FakeSelectionReader(_run(manifest_sha="sha_other")),
        ).bind(PackageBindingRequest(strategy_id="strat_a", package_id="pkg_a", selection_run_id="sel_a"))
