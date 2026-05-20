from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import qmt_strategy_ledger
from backend.services.qmt_strategy_ledger.models import (
    BindingStatus,
    CashEntryType,
    CashLedgerEntry,
    PositionLotRecord,
    StrategyPackageBinding,
    VirtualAccount,
    VirtualAccountStatus,
)
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.selection_center.models import SelectionMode, SelectionRun, SelectionRunStatus
from backend.services.strategy_package.live_inference import AUTHORITATIVE_SELECTION_SCOPE, AUTHORITATIVE_SELECTION_SOURCE_TYPE
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.selection_artifact import (
    InMemorySelectionScoreArtifactRepository,
    SelectionScoreArtifact,
    selection_artifact_runtime_hash,
)


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


def _selection_run(run_id: str, trade_date: date = TRADE_DATE) -> SelectionRun:
    return SelectionRun(
        run_id=run_id,
        mode=SelectionMode.SINGLE_PACKAGE,
        trade_date=trade_date,
        data_source="DB_HISTORICAL",
        package_ids=["pkg_a"],
        status=SelectionRunStatus.SUCCEEDED,
        manifest_sha256_by_package={"pkg_a": "sha_a"},
    )


def _artifact_repo(trade_date: date) -> InMemorySelectionScoreArtifactRepository:
    repo = InMemorySelectionScoreArtifactRepository()
    repo.save(
        SelectionScoreArtifact(
            package_id="pkg_a",
            manifest_sha256="sha_a",
            trade_date=trade_date,
            data_source="DB_HISTORICAL",
            runtime_config_hash=selection_artifact_runtime_hash({}),
            scores_json=[{"symbol": "300604.SZ", "score": 0.9, "rank": 1}],
            score_count=1,
            universe_count=1,
            top_score_symbol="300604.SZ",
            metadata={
                "source_type": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                "authority_scope": AUTHORITATIVE_SELECTION_SCOPE,
            },
        )
    )
    return repo


def _account(strategy_id: str, strategy_name: str) -> VirtualAccount:
    return VirtualAccount(
        strategy_id=strategy_id,
        strategy_name=strategy_name,
        display_name=strategy_name.replace("_", " "),
        account_id=ACCOUNT_ID,
        mode="SIM",
        initial_cash=Decimal("10000000"),
        cash=Decimal("9900000"),
        frozen_cash=Decimal("50000"),
        market_value=Decimal("100000"),
        realized_pnl=Decimal("1200"),
        unrealized_pnl=Decimal("800"),
        status=VirtualAccountStatus.ENABLED,
    )


def _lot(strategy_id: str, lot_id: str, quantity: int) -> PositionLotRecord:
    return PositionLotRecord(
        lot_id=lot_id,
        strategy_id=strategy_id,
        symbol="300604.SZ",
        open_trade_id=f"trade_{lot_id}",
        open_date=TRADE_DATE,
        quantity=quantity,
        available_quantity=0,
        remaining_quantity=quantity,
        avg_cost=Decimal("10.00"),
        cost_amount=Decimal(quantity * 10),
        account_id=ACCOUNT_ID,
    )


def _repo() -> InMemoryQmtStrategyLedgerRepository:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_virtual_account(_account("strat_a", "poc_strategy_a"))
    repo.create_virtual_account(_account("strat_b", "poc_strategy_b"))
    repo.create_position_lot(_lot("strat_a", "lot_a", 1000))
    repo.create_position_lot(_lot("strat_b", "lot_b", 500))
    repo.append_cash_entry(
        CashLedgerEntry(
            cash_id="cash_a",
            strategy_id="strat_a",
            entry_type=CashEntryType.INITIAL_ALLOCATE,
            cash_delta=Decimal("10000000"),
            cash_after=Decimal("10000000"),
            account_id=ACCOUNT_ID,
            trade_date=TRADE_DATE,
        )
    )
    repo.create_package_binding(
        StrategyPackageBinding(
            binding_id="bind_a",
            strategy_id="strat_a",
            package_id="pkg_a",
            manifest_sha256="sha_a",
            selection_run_id="sel_a",
            trade_date=TRADE_DATE,
            target_weight=Decimal("0.02"),
            top_k=20,
            binding_status=BindingStatus.ACTIVE,
        )
    )
    return repo


def _client(repo: InMemoryQmtStrategyLedgerRepository) -> TestClient:
    qmt_strategy_ledger.configure_dependencies(repository_factory=lambda: repo, client_factory=lambda: object())
    app = FastAPI()
    app.include_router(qmt_strategy_ledger.router, prefix="/api/v1")
    return TestClient(app)


def test_virtual_strategy_summary_exposes_accounts_lots_and_overlap() -> None:
    response = _client(_repo()).get(
        "/api/v1/qmt/virtual-strategies/summary",
        params={"account_id": ACCOUNT_ID, "trade_date": TRADE_DATE.isoformat()},
    )

    assert response.status_code == 200
    summary = response.json()["summary"]
    assert summary["strategy_count"] == 2
    assert summary["overlap_symbols"] == ["300604.SZ"]
    assert summary["unattributed_orders"] == 0
    assert summary["unattributed_trades"] == 0

    strat_a = next(row for row in summary["strategies"] if row["strategy_id"] == "strat_a")
    assert strat_a["active_binding"]["package_id"] == "pkg_a"
    assert strat_a["active_binding"]["selection_run_id"] == "sel_a"
    assert strat_a["positions"] == [
        {
            "symbol": "300604.SZ",
            "quantity": 1000,
            "available_quantity": 0,
            "remaining_quantity": 1000,
            "cost_amount": 10000.0,
            "avg_cost": 10.0,
            "realized_pnl": 0.0,
            "lot_count": 1,
        }
    ]


def test_package_binding_router_requires_explicit_replace_and_rolls_over_active_binding() -> None:
    repo = _repo()
    qmt_strategy_ledger.configure_dependencies(
        repository_factory=lambda: repo,
        client_factory=lambda: object(),
        package_reader_factory=lambda: FakePackageReader(FakePackageRecord("pkg_a", PackageStatus.SELECTION_ENABLED, "sha_a")),
        selection_reader_factory=lambda: FakeSelectionReader(_selection_run("sel_b", date(2026, 5, 19))),
        artifact_repository_factory=lambda: _artifact_repo(date(2026, 5, 19)),
    )
    app = FastAPI()
    app.include_router(qmt_strategy_ledger.router, prefix="/api/v1")
    client = TestClient(app)
    payload = {
        "strategy_id": "strat_a",
        "package_id": "pkg_a",
        "selection_run_id": "sel_b",
        "trade_date": "2026-05-19",
        "target_weight": "0.02",
        "top_k": 20,
    }

    rejected = client.post("/api/v1/qmt/virtual-strategies/package-bindings", json=payload)
    assert rejected.status_code == 409
    assert rejected.json()["detail"]["error_code"] == "INVALID_STATE_TRANSITION"

    replaced = client.post(
        "/api/v1/qmt/virtual-strategies/package-bindings",
        json={**payload, "replace_active": True, "replacement_reason": "next_day"},
    )

    body = replaced.json()
    assert replaced.status_code == 200
    assert body["action"] == "replaced_active"
    assert body["binding"]["selection_run_id"] == "sel_b"
    assert body["binding"]["trade_date"] == "2026-05-19"
    assert body["replaced_binding"]["binding_id"] == "bind_a"
    assert body["replaced_binding"]["binding_status"] == "RETIRED"
    active = repo.get_active_package_binding("strat_a")
    assert active.selection_run_id == "sel_b"
    assert active.runtime_config["frozen_runtime_asset"]["artifact_sha256"]


def test_package_binding_order_preview_fails_fast_until_minqmt_execution_bridge_exists() -> None:
    repo = _repo()
    response = _client(repo).post("/api/v1/qmt/virtual-strategies/package-bindings/bind_a/orders/preview", json={})

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["error_code"] == "UNSUPPORTED_FEATURE"
    assert detail["context"]["issue"] == "BUG-077"
    assert detail["context"]["disabled_path"] == "SelectionRun -> SelectionOrderBuilder -> ManagedOrderRequest"
    assert "validated execution policy" in detail["context"]["required_path"]
