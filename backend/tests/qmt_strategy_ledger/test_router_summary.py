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
    StrategyBindingSelectionEvidence,
    StrategyPackageBinding,
    VirtualAccount,
    VirtualAccountStatus,
)
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.selection_center.models import SelectionCandidate, SelectionMode, SelectionRun, SelectionRunStatus, SignalSnapshot
from backend.services.simulation_runtime import (
    DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
    DailySelectionEvidence,
    ExecutionPlanCompiler,
    InMemorySimulationRuntimeRepository,
    RebalanceIntentService,
    SimulationBrokerBackend,
    StrategyRuntimeReleaseService,
    TargetPositionService,
)
from backend.services.simulation_runtime.models import canonical_json_sha256
from backend.services.strategy_package.live_inference import AUTHORITATIVE_SELECTION_SCOPE, AUTHORITATIVE_SELECTION_SOURCE_TYPE
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.selection_artifact import (
    InMemorySelectionScoreArtifactRepository,
    SelectionScoreArtifact,
    selection_artifact_runtime_hash,
)
from backend.services.trading_core.models import PositionLot


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
    binding = repo.create_package_binding(
        StrategyPackageBinding(
            binding_id="bind_a",
            strategy_id="strat_a",
            package_id="pkg_a",
            manifest_sha256="sha_a",
            selection_run_id=None,
            trade_date=None,
            target_weight=Decimal("0.02"),
            top_k=20,
            binding_status=BindingStatus.ACTIVE,
        )
    )
    repo.record_binding_selection_evidence(
        StrategyBindingSelectionEvidence(
            evidence_id="ev_a",
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            package_id=binding.package_id,
            selection_run_id="sel_a",
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            manifest_sha256="sha_a",
            runtime_config_hash=selection_artifact_runtime_hash({}),
        )
    )
    return repo


def _client(repo: InMemoryQmtStrategyLedgerRepository) -> TestClient:
    qmt_strategy_ledger.configure_dependencies(repository_factory=lambda: repo, client_factory=lambda: object())
    app = FastAPI()
    app.include_router(qmt_strategy_ledger.router, prefix="/api/v1")
    return TestClient(app)


def _runtime_repo_with_miniqmt_plan() -> tuple[InMemorySimulationRuntimeRepository, str]:
    runtime_repo = InMemorySimulationRuntimeRepository()
    service = StrategyRuntimeReleaseService(repository=runtime_repo)
    release = service.create_release(
        package_id="pkg_a",
        manifest_sha256="sha_a",
        runtime_profile_id="runtime_profile_shared",
        runtime_profile_version_id="runtime_profile_shared_v1",
        runtime_profile_sha256="runtime_profile_hash_shared",
        daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        execution_policy_version_id="exec_policy_v25_1_small_cap",
        execution_policy_sha256="a" * 64,
        tail_policy_version_id="tail_policy_close_v1",
        tail_policy_sha256="tail_policy_hash_close_v1",
        created_by="unit-test",
        created_reason="qmt router shared execution plan preview",
    )
    binding = service.create_binding(
        strategy_id="strat_a",
        release=release,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        capital_allocation=10_000_000,
        broker_account_id=ACCOUNT_ID,
        strategy_name="poc_strategy_a",
        order_remark_prefix="shared-plan",
        miniqmt_quote_control={
            "schema_version": "miniqmt_quote_control_binding_v1",
            "control_revision": "B0_QUOTE_V2",
        },
        created_by="unit-test",
        created_reason="qmt router shared execution plan preview",
    )
    payload = {
        "schema_version": "daily_selection_evidence_v1",
        "target_trade_date": TRADE_DATE.isoformat(),
        "cutoff_date": "2026-05-17",
        "package_id": release.package_id,
        "manifest_sha256": release.manifest_sha256,
        "release_id": release.release_id,
        "release_hash": release.release_hash,
        "runtime_profile": {
            "profile_version_id": release.runtime_profile_version_id,
            "config_sha256": release.runtime_profile_sha256,
        },
        "source_type": "live_inference",
        "data_source": "DB_HISTORICAL",
        "candidates": [{"symbol": "300604.SZ", "score": 0.9, "rank": 1}],
        "exclusions": [],
    }
    digest = canonical_json_sha256(payload)
    evidence = runtime_repo.save_daily_selection_evidence(
        DailySelectionEvidence(
            evidence_id=f"dse_{digest[:16]}",
            target_trade_date=TRADE_DATE,
            cutoff_date=date(2026, 5, 17),
            package_id=release.package_id,
            manifest_sha256=release.manifest_sha256,
            release_id=release.release_id,
            release_hash=release.release_hash,
            runtime_profile_version_id=release.runtime_profile_version_id,
            runtime_profile_hash=release.runtime_profile_sha256,
            source_type="live_inference",
            data_source="DB_HISTORICAL",
            candidate_count=1,
            excluded_count=0,
            artifact_hash=digest,
            evidence_payload_json=payload,
            created_by="unit-test",
        )
    )
    snapshot = SignalSnapshot(
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        candidates=[
            SelectionCandidate(
                symbol="300604.SZ",
                score=0.9,
                rank=1,
                target_quantity=1500,
                target_weight=0.02,
                reference_price=10.0,
                reason="daily_strategy_buy_or_retain",
            )
        ],
        runtime_config={"runtime_profile": {"selection": {"daily_strategy_id": DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID}}},
    )
    current_positions = {
        "300604.SZ": PositionLot(
            portfolio_id="strat_a",
            symbol="300604.SZ",
            quantity=1000,
            available_quantity=0,
            avg_cost=10.0,
            trade_date=date(2026, 5, 17),
        )
    }
    targets = TargetPositionService().build_target_positions(
        selection_evidence=evidence,
        signal_snapshot=snapshot,
        runtime_release=release,
        binding=binding,
        current_positions=current_positions,
    )
    rebalance = RebalanceIntentService().build_order_intents(
        package_id=release.package_id,
        portfolio_id="strat_a",
        strategy_id=binding.strategy_id,
        trade_date=TRADE_DATE,
        current_positions=current_positions,
        target_positions=targets,
    )
    plan = ExecutionPlanCompiler().compile_plan(
        runtime_release=release,
        binding=binding,
        selection_evidence=evidence,
        order_intents=rebalance.order_intents,
        trading_rule_decisions=rebalance.trading_rule_decisions,
        portfolio_id="strat_a",
        execution_policy_payload={
            "algo_code": "SNIPER_MINIQMT",
            "algo_config": {
                "tca": {
                    "benchmark_policy": {
                        "benchmark_max_age_ms": 10_000,
                        "arrival_forward_window_ms": 2_000,
                        "clock_skew_tolerance_ms": 1_000,
                        "benchmark_max_transport_latency_ms": 3_000,
                        "policy_version": "miniqmt_execution_tca_benchmark_v1",
                    }
                }
            },
            "quote_contract": {
                "schema_version": "miniqmt_quote_contract_policy_v2",
                "control_revision": "B0_QUOTE_V2",
                "required_capabilities": [
                    "CALENDAR",
                    "DEPTH_UNIT_SHARES",
                    "EXCHANGE_TIMESTAMP",
                    "FIVE_LEVEL_DEPTH",
                    "RAW_PRICE_BASIS",
                    "TRADABILITY",
                ],
                "max_receive_age_ms": 20_000,
                "max_source_lag_ms": 20_000,
                "max_exchange_age_ms": 20_000,
                "max_negative_skew_ms": 1_000,
                "max_clock_age_divergence_ms": 1_000,
                "max_dependency_group_skew_ms": 20_000,
                "auction_mode": "OBSERVE_ONLY",
            },
            "quote_evidence": {
                "schema_version": "miniqmt_quote_evidence_policy_v1",
                "benchmark_policy_version": "miniqmt_execution_tca_benchmark_v1",
                "mark_policy_version": "miniqmt_execution_tca_mark_selector_v1",
                "markout_max_lag_ms": 10_000,
            },
            "schedule_window": {"mode": "open_to_close"},
        },
    )
    return runtime_repo, runtime_repo.save_execution_plan(plan).plan_id


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
    assert strat_a["active_binding"]["selection_run_id"] is None
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


def test_package_binding_router_records_daily_selection_without_replacing_active_binding() -> None:
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

    recorded = client.post("/api/v1/qmt/virtual-strategies/package-bindings", json=payload)

    body = recorded.json()
    assert recorded.status_code == 200
    assert body["action"] == "daily_selection_recorded"
    assert body["binding"]["selection_run_id"] is None
    assert body["binding"]["trade_date"] is None
    assert body["replaced_binding"] is None
    assert body["daily_selection_evidence"]["selection_run_id"] == "sel_b"
    assert body["daily_selection_evidence"]["trade_date"] == "2026-05-19"
    active = repo.get_active_package_binding("strat_a")
    assert active.binding_id == "bind_a"
    evidence = repo.get_binding_selection_evidence(active.binding_id, date(2026, 5, 19))
    assert evidence.selection_run_id == "sel_b"
    assert evidence.artifact_sha256


def test_package_binding_order_preview_fails_fast_until_minqmt_execution_bridge_exists() -> None:
    repo = _repo()
    response = _client(repo).post("/api/v1/qmt/virtual-strategies/package-bindings/bind_a/orders/preview", json={})

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["error_code"] == "UNSUPPORTED_FEATURE"
    assert detail["context"]["issue"] == "BUG-077"
    assert detail["context"]["disabled_path"] == "SelectionRun -> SelectionOrderBuilder -> ManagedOrderRequest"
    assert "validated execution policy" in detail["context"]["required_path"]


def test_execution_plan_order_preview_does_not_reopen_retired_miniqmt_compiler_route() -> None:
    qmt_repo = _repo()
    runtime_repo, plan_id = _runtime_repo_with_miniqmt_plan()
    qmt_strategy_ledger.configure_dependencies(
        repository_factory=lambda: qmt_repo,
        client_factory=lambda: object(),
        simulation_runtime_repository_factory=lambda: runtime_repo,
    )
    app = FastAPI()
    app.include_router(qmt_strategy_ledger.router, prefix="/api/v1")

    response = TestClient(app).post(f"/api/v1/qmt/virtual-strategies/execution-plans/{plan_id}/orders/preview", json={})

    body = response.json()
    assert response.status_code == 400, body
    assert body["detail"]["context"]["reason_code"] == "MINIQMT_EVENT_LOOP_REQUIRES_REAL_CALLBACKS"
    assert body["detail"]["context"]["operation"] == "build_managed_vnpy_order_requests"
