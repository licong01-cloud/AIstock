from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from backend.services.paper_trading_v2.models import PaperPortfolio
from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.broker.base import OrderHandle
from backend.services.qmt_strategy_ledger.lot_availability import StaticTradingCalendarProvider
from backend.services.qmt_strategy_ledger.models import (
    IntentSubmitStatus,
    PositionLotRecord,
    VirtualAccount,
    VirtualAccountStatus,
)
from backend.services.qmt_strategy_ledger.order_service import ManagedOrderRequest, QmtManagedOrderService
from backend.services.qmt_strategy_ledger.reconciliation import QmtStrategyLedgerReconciliationService
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.qmt_strategy_ledger.sync_service import QmtStrategyLedgerSyncService
from backend.services.selection_center.models import SelectionCandidate
from backend.services.simulation_runtime import (
    DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
    DailySelectionEvidence,
    InMemorySimulationRuntimeRepository,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
    SimulationLifecycleBackgroundScheduler,
    SimulationDailyRunStatus,
    SimulationLifecycleScheduler,
    SimulationRunContext,
    StaticSimulationRunContextProvider,
    StrategyPackageSelectionResult,
    StrategyRuntimeReleaseService,
)
from backend.services.simulation_runtime.models import canonical_json_sha256
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import (
    AlphaCombinationPolicy,
    AlphaComponent,
    AlphaMode,
    BacktestSummary,
    FactorAsset,
    ModelAsset,
    PackageStatus,
    SourceType,
    StrategyPackageManifest,
    StrategyPackageSource,
)
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.models import PositionLot


TRADE_DATE = date(2026, 5, 21)


def _release_and_bindings(*, qmt_only: bool = False, release_metadata: dict | None = None):
    repo = InMemorySimulationRuntimeRepository()
    service = StrategyRuntimeReleaseService(repository=repo)
    release = service.create_release(
        package_id="pkg_scheduler",
        manifest_sha256="manifest_scheduler",
        runtime_profile_id="runtime_profile_scheduler",
        runtime_profile_version_id="runtime_profile_scheduler_v1",
        runtime_profile_sha256="runtime_profile_scheduler_hash",
        daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        execution_policy_version_id="exec_policy_v25_1_small_cap",
        execution_policy_sha256="exec_policy_hash_v25_1_small_cap",
        tail_policy_version_id="tail_policy_close_v1",
        tail_policy_sha256="tail_policy_hash_close_v1",
        release_metadata=release_metadata,
        created_by="unit-test",
        created_reason="scheduler test",
    )
    qmt_binding = service.create_binding(
        strategy_id="strategy_qmt_scheduler",
        release=release,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        capital_allocation=100_000,
        broker_account_id="QMT_SIM_ACCOUNT",
        strategy_name="SchedulerQMT",
        order_remark_prefix="sched-qmt",
        approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
        created_by="unit-test",
        created_reason="scheduler test",
    )
    if qmt_only:
        return release, None, qmt_binding, repo
    local_binding = service.create_binding(
        strategy_id="strategy_local_scheduler",
        release=release,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        capital_allocation=100_000,
        approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
        created_by="unit-test",
        created_reason="scheduler test",
    )
    return release, local_binding, qmt_binding, repo


def _create_extra_binding(
    *,
    release,
    repo: InMemorySimulationRuntimeRepository,
    strategy_id: str,
    broker_backend: SimulationBrokerBackend,
    broker_account_id: str | None = None,
    strategy_name: str | None = None,
    order_remark_prefix: str | None = None,
):
    return StrategyRuntimeReleaseService(repository=repo).create_binding(
        strategy_id=strategy_id,
        release=release,
        broker_backend=broker_backend,
        capital_allocation=100_000,
        broker_account_id=broker_account_id,
        strategy_name=strategy_name,
        order_remark_prefix=order_remark_prefix,
        approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
        created_by="unit-test",
        created_reason="multi strategy scheduler test",
    )


def _candidate_rows() -> list[SelectionCandidate]:
    return [
        SelectionCandidate(
            symbol="000001.SZ",
            score=0.99,
            rank=1,
            target_quantity=1000,
            target_weight=0.10,
            reference_price=10.0,
            reason="daily_strategy_buy_or_retain",
        ),
        SelectionCandidate(
            symbol="688001.SH",
            score=0.98,
            rank=2,
            target_quantity=201,
            target_weight=0.04,
            reference_price=20.0,
            reason="daily_strategy_buy_or_retain",
        ),
    ]


def _evidence(release, *, candidates: list[SelectionCandidate], valid_no_candidate: bool = False) -> DailySelectionEvidence:
    payload = {
        "schema_version": "daily_selection_evidence_v1",
        "target_trade_date": TRADE_DATE.isoformat(),
        "cutoff_date": "2026-05-20",
        "package_id": release.package_id,
        "manifest_sha256": release.manifest_sha256,
        "release_id": release.release_id,
        "release_hash": release.release_hash,
        "runtime_profile_version_id": release.runtime_profile_version_id,
        "runtime_profile_hash": release.runtime_profile_sha256,
        "source_type": "live_inference",
        "data_source": "DB_HISTORICAL",
        "selected_candidates": [item.model_dump(mode="json") for item in candidates],
        "excluded_candidates": [],
        "valid_no_candidate": valid_no_candidate,
        "no_candidate_reason": "unit test no candidate day" if valid_no_candidate else None,
    }
    digest = canonical_json_sha256(payload)
    return DailySelectionEvidence(
        evidence_id=f"dse_{digest[:16]}",
        target_trade_date=TRADE_DATE,
        cutoff_date=date(2026, 5, 20),
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        release_id=release.release_id,
        release_hash=release.release_hash,
        runtime_profile_version_id=release.runtime_profile_version_id,
        runtime_profile_hash=release.runtime_profile_sha256,
        source_type="live_inference",
        data_source="DB_HISTORICAL",
        candidate_count=len(candidates),
        excluded_count=0,
        artifact_hash=digest,
        evidence_payload_json=payload,
        created_by="unit-test",
    )


class FakeSelectionService:
    def __init__(self, release, *, candidates: list[SelectionCandidate] | None = None, valid_no_candidate: bool = False) -> None:
        self.release = release
        self.candidates = list(candidates or [])
        self.valid_no_candidate = valid_no_candidate
        self.calls: list[dict] = []

    def run_selection(self, **kwargs):
        self.calls.append(kwargs)
        evidence = _evidence(
            self.release,
            candidates=self.candidates,
            valid_no_candidate=self.valid_no_candidate,
        )
        no_candidate_reason = "unit test no candidate day" if self.valid_no_candidate else None
        return StrategyPackageSelectionResult(
            runtime_config={
                "runtime_profile": {
                    "selection": {"daily_strategy_id": DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID}
                }
            },
            package_results={self.release.package_id: self.candidates},
            aggregate_results=self.candidates,
            excluded_results={self.release.package_id: []},
            manifest_sha256_by_package={self.release.package_id: self.release.manifest_sha256},
            evidence_by_package={self.release.package_id: evidence},
            valid_no_candidate=self.valid_no_candidate,
            no_candidate_reason=no_candidate_reason,
        )


class CountingContextProvider(StaticSimulationRunContextProvider):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.calls: list[str] = []

    def load_context(self, *, runtime_release, binding, trade_date):
        self.calls.append(binding.binding_id)
        return super().load_context(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
        )


def _position_context(*, portfolio_id: str, local_broker=None) -> SimulationRunContext:
    return SimulationRunContext(
        portfolio_id=portfolio_id,
        current_positions={
            "000001.SZ": PositionLot(
                portfolio_id=portfolio_id,
                symbol="000001.SZ",
                quantity=1000,
                available_quantity=1000,
                avg_cost=9.5,
                trade_date=date(2026, 5, 20),
            ),
            "000003.SZ": PositionLot(
                portfolio_id=portfolio_id,
                symbol="000003.SZ",
                quantity=77,
                available_quantity=77,
                avg_cost=8.0,
                trade_date=date(2026, 5, 20),
            ),
        },
        current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0},
        local_broker=local_broker,
    )


class FakeLocalSimBroker:
    def __init__(self) -> None:
        self.submitted = []

    def submit_order_intent(self, intent):
        self.submitted.append(intent)
        return OrderHandle(
            handle_id=f"local_{len(self.submitted)}",
            backend_id="local_sim",
            submitted_at=datetime.now(UTC),
            intent_id=intent.intent_id,
        )


class FakeManagedOrderBroker:
    def __init__(self, order_ids: list[int] | None = None, positions: list[dict[str, Any]] | None = None) -> None:
        self.order_ids = list(order_ids or [])
        self.positions = list(positions or [{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
        self.place_order_payloads = []

    def get_positions(self):
        return list(self.positions)

    def place_order(self, **kwargs):
        self.place_order_payloads.append(kwargs)
        order_id = self.order_ids.pop(0) if self.order_ids else 900000000 + len(self.place_order_payloads)
        return order_id, "accepted" if order_id > 0 else "rejected by fake broker"

    def cancel_order(self, order_id: str):
        return True, f"cancelled {order_id}"


class FakeQmtSnapshotClient:
    def __init__(
        self,
        *,
        orders: list[dict[str, Any]] | None = None,
        trades: list[dict[str, Any]] | None = None,
        positions: list[dict[str, Any]] | None = None,
    ) -> None:
        self.calls: list[str] = []
        self._orders = list(orders or [])
        self._trades = list(trades or [])
        self._positions = list(positions or [])

    def get_orders(self, cancelable_only: bool = False) -> list[dict[str, Any]]:
        self.calls.append(f"orders:{cancelable_only}")
        return list(self._orders)

    def get_trades(self) -> list[dict[str, Any]]:
        self.calls.append("trades")
        return list(self._trades)

    def get_positions(self) -> list[dict[str, Any]]:
        self.calls.append("positions")
        return list(self._positions)


class FakePaperRepository:
    def __init__(self, portfolio: PaperPortfolio, *, positions: dict[str, PositionLot], cash: float) -> None:
        self.portfolio = portfolio
        self.positions = dict(positions)
        self.cash = cash
        self.calls: list[tuple[str, str, date]] = []

    def get_portfolio(self, portfolio_id: str) -> PaperPortfolio:
        self.calls.append(("get_portfolio", portfolio_id, TRADE_DATE))
        if portfolio_id != self.portfolio.portfolio_id:
            raise DataUnavailableError("paper v2 portfolio does not exist", context={"portfolio_id": portfolio_id})
        return self.portfolio

    def load_latest_positions(self, portfolio_id: str, before_or_on: date) -> dict[str, PositionLot]:
        self.calls.append(("load_latest_positions", portfolio_id, before_or_on))
        return dict(self.positions)

    def load_latest_cash(self, portfolio: PaperPortfolio, before_or_on: date) -> float:
        self.calls.append(("load_latest_cash", portfolio.portfolio_id, before_or_on))
        return self.cash


def _frozen_manifest(package_id: str = "pkg_scheduler", manifest_sha256: str | None = None) -> StrategyPackageManifest:
    manifest = StrategyPackageManifest(
        manifest_version="alpha_core_v1",
        package_id=package_id,
        package_name="Scheduler test package",
        source=StrategyPackageSource(
            source_type=SourceType.QE_EXPERIMENT,
            source_id="qe_scheduler",
        ),
        alpha_mode=AlphaMode.SINGLE_ALPHA,
        alpha_components=[
            AlphaComponent(
                alpha_id="alpha_scheduler",
                alpha_name="Scheduler Alpha",
                component_weight=1.0,
                factor_ids=["factor_scheduler"],
                model_id="model_scheduler",
                holding_period="1d",
                rebalance_frequency="1d",
                score_direction="higher_better",
            )
        ],
        alpha_combination_policy=AlphaCombinationPolicy(
            method="identity",
            weights={"alpha_scheduler": 1.0},
            conflict_resolution="highest_score",
        ),
        factor_set=[FactorAsset(factor_id="factor_scheduler", factor_name="factor_scheduler")],
        model_asset=ModelAsset(model_id="model_scheduler"),
        backtest_summary=BacktestSummary(ic=0.03, rank_ic=0.02, raw_metrics={"IC": 0.03}),
        package_status=PackageStatus.PAPER_ENABLED,
    )
    frozen = freeze_manifest(manifest)
    if manifest_sha256 is not None:
        frozen = frozen.model_copy(update={"manifest_sha256": manifest_sha256})
    return frozen


def test_scheduler_plans_active_local_and_miniqmt_bindings_from_same_selection_evidence() -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings()
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _position_context(portfolio_id="portfolio_shared"),
                qmt_binding.binding_id: _position_context(portfolio_id="portfolio_shared"),
            }
        ),
    )

    result = scheduler.run_once(trade_date=TRADE_DATE, data_source="DB_HISTORICAL", submit=False)

    assert result.total_bindings == 2
    assert result.planned_count == 2
    assert result.failed_count == 0
    plans = [item.execution_plan for item in result.results]
    assert {plan.selection_evidence_hash for plan in plans if plan is not None} == {
        plans[0].selection_evidence_hash
    }
    normalized_intents = [
        [(intent.symbol, intent.side.value, intent.order_quantity, intent.rebalance_reason) for intent in plan.intents]
        for plan in plans
        if plan is not None
    ]
    assert normalized_intents[0] == normalized_intents[1]
    assert ("000003.SZ", "SELL", 77, "DROPPED_FROM_SELECTION") in normalized_intents[0]


def test_scheduler_passes_release_selection_runtime_config_to_selection_service() -> None:
    release_selection_config = {
        "selection_artifact_config": {
            "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE",
            "cutoff_date": "2026-05-20",
            "include_reference_price": True,
            "artifact_reuse": "same_trade_date_config_hash",
        },
        "runtime_profile": {
            "selection": {"top_k": 2},
            "tradability": {"exclude_suspended": False},
        },
    }
    release, local_binding, _, repo = _release_and_bindings(
        release_metadata={"selection_runtime_config": release_selection_config}
    )
    assert local_binding is not None
    fake_selection = FakeSelectionService(release, candidates=_candidate_rows())
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=fake_selection,
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_release_config")}
        ),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )

    assert result.planned_count == 1
    assert len(fake_selection.calls) == 1
    assert fake_selection.calls[0]["runtime_config"] == release_selection_config


def test_scheduler_reuses_existing_plans_on_restart_without_reselection_or_resubmit() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    fake_selection = FakeSelectionService(release, candidates=_candidate_rows())
    broker = FakeLocalSimBroker()
    context_provider = CountingContextProvider(
        by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_shared", local_broker=broker)}
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=fake_selection,
        context_provider=context_provider,
    )

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )
    assert first.results[0].status == "SUBMITTED"
    assert first.results[0].run.run_payload_json["broker_called"] is True
    broker.submitted.clear()

    restarted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    assert restarted.reused_count == 1
    assert len(fake_selection.calls) == 1
    assert context_provider.calls == [local_binding.binding_id]
    assert broker.submitted == []
    assert restarted.results[0].execution_plan.plan_id == first.results[0].execution_plan.plan_id


def test_scheduler_submits_existing_local_plan_after_restart_when_broker_was_not_called() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    fake_selection = FakeSelectionService(release, candidates=_candidate_rows())
    plan_only_context = StaticSimulationRunContextProvider(
        by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_shared")}
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=fake_selection,
        context_provider=plan_only_context,
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    broker = FakeLocalSimBroker()
    scheduler.context_provider = CountingContextProvider(
        by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_shared", local_broker=broker)}
    )
    restarted_context = scheduler.context_provider

    restarted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    assert planned.planned_count == 1
    assert restarted.results[0].status == "SUBMITTED"
    assert len(fake_selection.calls) == 1
    assert restarted_context.calls == [local_binding.binding_id]
    assert [intent.intent_id for intent in broker.submitted] == [
        intent.intent_id for intent in planned.results[0].execution_plan.intents
    ]
    assert restarted.results[0].run.run_payload_json["broker_called"] is True
    assert restarted.results[0].run.run_payload_json["broker_order_handles"][0]["backend_id"] == "local_sim"


def test_scheduler_recovers_submitting_local_plan_when_broker_was_not_called() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    fake_selection = FakeSelectionService(release, candidates=_candidate_rows())
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=fake_selection,
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_shared")}
        ),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    interrupted = repo.update_simulation_daily_run(
        planned.results[0].run.run_id,
        status=SimulationDailyRunStatus.SUBMITTING,
        payload_patch={"last_stage": "SUBMITTING"},
    )
    assert interrupted.run_payload_json.get("broker_called") is None

    broker = FakeLocalSimBroker()
    scheduler.context_provider = CountingContextProvider(
        by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_shared", local_broker=broker)}
    )
    recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    assert recovered.results[0].status == "SUBMITTED"
    assert recovered.results[0].run.status == SimulationDailyRunStatus.SUCCEEDED
    assert [intent.intent_id for intent in broker.submitted] == [
        intent.intent_id for intent in planned.results[0].execution_plan.intents
    ]
    assert recovered.results[0].run.run_payload_json["broker_called"] is True
    assert recovered.results[0].run.run_payload_json["last_stage"] == "SUCCEEDED"


def test_scheduler_submits_miniqmt_fake_broker_batch_and_reuses_after_restart() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_000003",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_000003",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = FakeQmtSnapshotClient(
        positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}]
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(
                        repository=qmt_repo,
                        broker=broker,  # type: ignore[arg-type]
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_ledger_repository=qmt_repo,
                    qmt_sync_service=QmtStrategyLedgerSyncService(
                        repository=qmt_repo,
                        qmt_client=snapshot_client,
                        account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                        trade_date=TRADE_DATE,
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
                    broker_positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
                )
            }
        ),
    )

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    restarted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    assert submitted.submitted_count == 1
    assert submitted.results[0].status == "RECONCILED"
    assert submitted.results[0].run.status == SimulationDailyRunStatus.SUCCEEDED
    assert submitted.results[0].sync_result["positions_seen"] == 1
    latest_run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    assert latest_run.run_payload_json["strategy_performance"]["broker_backend"] == "minqmt_sim"
    assert latest_run.run_payload_json["strategy_performance"]["positions"][0]["symbol"] == "000003.SZ"
    assert submitted.results[0].run.run_payload_json["qmt_batch_status"] == "SUCCEEDED"
    assert [call["strategy_name"] for call in broker.place_order_payloads] == [
        qmt_binding.strategy_name,
        qmt_binding.strategy_name,
    ]
    assert restarted.reused_count == 1
    assert len(broker.place_order_payloads) == 2


def test_scheduler_miniqmt_restart_syncs_before_submit_and_reconciles_after_submit() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_000003_restart",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_000003_restart",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = FakeQmtSnapshotClient(
        positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}]
    )
    managed_order_service = QmtManagedOrderService(
        repository=qmt_repo,
        broker=broker,  # type: ignore[arg-type]
        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=managed_order_service,
                    qmt_ledger_repository=qmt_repo,
                    qmt_sync_service=QmtStrategyLedgerSyncService(
                        repository=qmt_repo,
                        qmt_client=snapshot_client,
                        account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                        trade_date=TRADE_DATE,
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
                    broker_positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
                )
            }
        ),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    restarted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    assert planned.planned_count == 1
    assert submitted.results[0].status == "RECONCILED"
    assert submitted.results[0].run.status == SimulationDailyRunStatus.SUCCEEDED
    assert submitted.results[0].sync_result["positions_seen"] == 1
    assert submitted.results[0].reconciliation_result["run"]["status"] == "SUCCEEDED"
    assert repo.get_simulation_daily_run(submitted.results[0].run.run_id).run_payload_json["strategy_performance"]["nav"] > 0
    assert submitted.results[0].run.run_payload_json["sync_before_submit"]["orders_seen"] == 0
    assert submitted.results[0].run.run_payload_json["reconcile_after_submit"]["broker_quantities"] == {
        "000003.SZ": 77
    }
    assert restarted.reused_count == 1
    assert len(broker.place_order_payloads) == 2


def test_scheduler_recovers_called_miniqmt_retryable_run_by_reconcile_only() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_recover_called",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_recover_called",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = FakeQmtSnapshotClient(
        positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}]
    )
    managed_order_service = QmtManagedOrderService(
        repository=qmt_repo,
        broker=broker,  # type: ignore[arg-type]
        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
    )

    def context_with_positions(quantity: int) -> SimulationRunContext:
        return SimulationRunContext(
            portfolio_id="portfolio_qmt",
            current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
            current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
            managed_order_service=managed_order_service,
            qmt_ledger_repository=qmt_repo,
            qmt_sync_service=QmtStrategyLedgerSyncService(
                repository=qmt_repo,
                qmt_client=snapshot_client,
                account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                trade_date=TRADE_DATE,
                calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
            ),
            qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
            broker_positions=[{"stock_code": "000003.SZ", "quantity": quantity, "can_sell": quantity}],
        )

    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={qmt_binding.binding_id: context_with_positions(1)}
        ),
    )
    failed = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    placed_count = len(broker.place_order_payloads)
    failed_run = repo.get_simulation_daily_run(failed.results[0].run.run_id)
    assert failed.results[0].status == "RECONCILIATION_WARNING"
    assert failed_run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert failed_run.run_payload_json["broker_called"] is True
    assert failed_run.run_payload_json["qmt_batch_status"] == "SUCCEEDED"

    scheduler.context_provider = StaticSimulationRunContextProvider(
        by_binding_id={qmt_binding.binding_id: context_with_positions(77)}
    )
    recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    recovered_run = repo.get_simulation_daily_run(failed_run.run_id)
    assert recovered.results[0].status == "RECONCILED"
    assert recovered_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert recovered_run.run_payload_json["reconcile_after_submit"]["run"]["status"] == "SUCCEEDED"
    assert len(broker.place_order_payloads) == placed_count


def test_scheduler_miniqmt_reconcile_warning_marks_run_retryable() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = FakeQmtSnapshotClient(
        positions=[{"stock_code": "000003.SZ", "quantity": 1, "can_sell": 1}]
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(
                        repository=qmt_repo,
                        broker=broker,  # type: ignore[arg-type]
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_ledger_repository=qmt_repo,
                    qmt_sync_service=QmtStrategyLedgerSyncService(
                        repository=qmt_repo,
                        qmt_client=snapshot_client,
                        account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                        trade_date=TRADE_DATE,
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
                    broker_positions=[{"stock_code": "000003.SZ", "quantity": 1, "can_sell": 1}],
                )
            }
        ),
    )

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    assert submitted.results[0].status == "RECONCILIATION_WARNING"
    assert submitted.results[0].run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert submitted.results[0].run.run_payload_json["last_stage"] == "FAILED_RETRYABLE"
    assert submitted.results[0].run.run_payload_json["reconcile_after_submit"]["issues"][0]["issue_type"] == "POSITION_MISMATCH"


def test_scheduler_broker_backend_filter_limits_tick_scope() -> None:
    release, _, qmt_binding, repo = _release_and_bindings()
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={qmt_binding.binding_id: _position_context(portfolio_id="portfolio_qmt")}
        ),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )

    assert result.total_bindings == 1
    assert result.results[0].broker_backend == SimulationBrokerBackend.MINIQMT_SIM
    assert result.results[0].binding_id == qmt_binding.binding_id


def test_scheduler_reports_unattended_trading_windows_without_submitting_orders() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_window")}
        ),
    )

    status = scheduler.status()
    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22, tzinfo=UTC),
    )

    assert status["restart_recovery_mode"] == "persisted_state_only"
    assert [window["window_id"] for window in status["schedule_windows"]] == [
        "pre_open",
        "selection",
        "planning",
        "execution",
    ]
    assert result.planned_count == 1
    assert result.schedule_windows[2]["window_id"] == "planning"
    assert result.schedule_windows[2]["state"] == "ACTIVE"
    assert result.schedule_windows[3]["state"] == "UPCOMING"


def test_background_scheduler_runs_planning_window_and_keeps_submit_disabled_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings()
    assert local_binding is not None
    lifecycle = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _position_context(portfolio_id="portfolio_background_local"),
                qmt_binding.binding_id: _position_context(portfolio_id="portfolio_background_qmt"),
            }
        ),
    )
    monkeypatch.setenv("SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT", "false")
    background = SimulationLifecycleBackgroundScheduler(lifecycle_scheduler=lifecycle)

    result = background.run_once(as_of_time=datetime(2026, 5, 21, 9, 22, tzinfo=UTC))

    assert result["should_run"] is True
    assert result["submit"] is False
    assert result["window"]["window_id"] == "planning"
    assert result["summary"]["planned_count"] == 2
    assert background.status()["default_submit"] is False
    assert background.status()["last_result"]["summary"]["total_bindings"] == 2


def test_scheduler_no_rebalance_submission_marks_success_without_broker() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=[], valid_no_candidate=True),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_empty",
                    current_positions={},
                )
            }
        ),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    assert result.submitted_count == 1
    assert result.results[0].status == "NO_REBALANCE"
    assert result.results[0].run.status == SimulationDailyRunStatus.SUCCEEDED
    assert result.results[0].run.run_payload_json["broker_called"] is False


def test_scheduler_runs_two_localsim_strategies_with_independent_state_and_restart_idempotency() -> None:
    release, local_binding_a, _, repo = _release_and_bindings()
    assert local_binding_a is not None
    local_binding_b = _create_extra_binding(
        release=release,
        repo=repo,
        strategy_id="strategy_local_scheduler_b",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
    )
    broker_a = FakeLocalSimBroker()
    broker_b = FakeLocalSimBroker()
    selection = FakeSelectionService(release, candidates=_candidate_rows())
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=selection,
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding_a.binding_id: _position_context(
                    portfolio_id="portfolio_local_a",
                    local_broker=broker_a,
                ),
                local_binding_b.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_local_b",
                    current_positions={
                        "000001.SZ": PositionLot(
                            portfolio_id="portfolio_local_b",
                            symbol="000001.SZ",
                            quantity=400,
                            available_quantity=400,
                            avg_cost=9.8,
                            trade_date=date(2026, 5, 20),
                        ),
                        "000004.SZ": PositionLot(
                            portfolio_id="portfolio_local_b",
                            symbol="000004.SZ",
                            quantity=200,
                            available_quantity=200,
                            avg_cost=6.0,
                            trade_date=date(2026, 5, 20),
                        ),
                    },
                    current_prices={"000001.SZ": 10.0, "000004.SZ": 6.0},
                    local_broker=broker_b,
                ),
            }
        ),
    )

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )
    submitted_by_strategy = {item.strategy_id: item for item in submitted.results}
    broker_a.submitted.clear()
    broker_b.submitted.clear()
    restarted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    assert submitted.total_bindings == 2
    assert submitted.failed_count == 0
    assert set(submitted_by_strategy) == {local_binding_a.strategy_id, local_binding_b.strategy_id}
    assert len({item.execution_plan.plan_id for item in submitted_by_strategy.values()}) == 2
    assert submitted_by_strategy[local_binding_a.strategy_id].run.run_payload_json["strategy_performance"]["initial_capital"] == 100000.0
    assert submitted_by_strategy[local_binding_b.strategy_id].run.run_payload_json["strategy_performance"]["positions"][0]["symbol"] == "000001.SZ"
    assert len(selection.calls) == 2
    assert restarted.reused_count == 2
    assert broker_a.submitted == []
    assert broker_b.submitted == []


def test_scheduler_miniqmt_two_strategies_same_stock_keep_strategy_lots_and_merged_reconcile() -> None:
    release, _, qmt_binding_a, repo = _release_and_bindings(qmt_only=True)
    qmt_binding_b = _create_extra_binding(
        release=release,
        repo=repo,
        strategy_id="strategy_qmt_scheduler_b",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        broker_account_id=qmt_binding_a.broker_account_id,
        strategy_name="SchedulerQMTB",
        order_remark_prefix="sched-qmt-b",
    )
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    for binding in (qmt_binding_a, qmt_binding_b):
        qmt_repo.create_virtual_account(
            VirtualAccount(
                strategy_id=binding.strategy_id,
                strategy_name=binding.strategy_name or binding.strategy_id,
                display_name=binding.strategy_name or binding.strategy_id,
                account_id=binding.broker_account_id or "QMT_SIM_ACCOUNT",
                mode="SIM",
                initial_cash=Decimal("100000"),
                cash=Decimal("100000"),
                status=VirtualAccountStatus.ENABLED,
            )
        )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_a_000003",
            strategy_id=qmt_binding_a.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_a_000003",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding_a.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_b_000003",
            strategy_id=qmt_binding_b.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_b_000003",
            open_date=date(2026, 5, 20),
            quantity=123,
            available_quantity=123,
            remaining_quantity=123,
            avg_cost=Decimal("8.10"),
            cost_amount=Decimal("996.30"),
            account_id=qmt_binding_b.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker(positions=[{"stock_code": "000003.SZ", "quantity": 200, "can_sell": 200}])
    calendar = StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE])
    snapshot_client = FakeQmtSnapshotClient(
        positions=[{"stock_code": "000003.SZ", "quantity": 200, "can_sell": 200}]
    )
    context_by_binding = {}
    for binding, quantity in ((qmt_binding_a, 77), (qmt_binding_b, 123)):
        context_by_binding[binding.binding_id] = SimulationRunContext(
            portfolio_id=f"portfolio_{binding.strategy_id}",
            current_positions={
                "000003.SZ": PositionLot(
                    portfolio_id=f"portfolio_{binding.strategy_id}",
                    symbol="000003.SZ",
                    quantity=quantity,
                    available_quantity=quantity,
                    avg_cost=8.0,
                    trade_date=date(2026, 5, 20),
                )
            },
            current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
            managed_order_service=QmtManagedOrderService(
                repository=qmt_repo,
                broker=broker,  # type: ignore[arg-type]
                calendar_provider=calendar,
            ),
            qmt_ledger_repository=qmt_repo,
            qmt_sync_service=QmtStrategyLedgerSyncService(
                repository=qmt_repo,
                qmt_client=snapshot_client,
                account_id=binding.broker_account_id or "QMT_SIM_ACCOUNT",
                trade_date=TRADE_DATE,
                calendar_provider=calendar,
            ),
            qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
            broker_positions=[{"stock_code": "000003.SZ", "quantity": 200, "can_sell": 200}],
        )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id=context_by_binding),
    )

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    assert submitted.total_bindings == 2
    assert submitted.failed_count == 0
    for item in submitted.results:
        payload = item.run.run_payload_json["reconcile_after_submit"]
        assert payload["run"]["status"] == "SUCCEEDED"
        assert payload["broker_quantities"] == {"000003.SZ": 200}
        assert payload["overlap_symbols"] == ["000003.SZ"]
        assert payload["strategy_lot_quantities"]["SchedulerQMT"]["000003.SZ"] == 77
        assert payload["strategy_lot_quantities"]["SchedulerQMTB"]["000003.SZ"] == 123
    assert len(broker.place_order_payloads) == 6
    assert [payload["strategy_name"] for payload in broker.place_order_payloads].count("SchedulerQMT") == 3
    assert [payload["strategy_name"] for payload in broker.place_order_payloads].count("SchedulerQMTB") == 3


def test_production_context_provider_loads_positions():
    """Production provider returns context with positions from the loader."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider
    from backend.services.trading_core.models import PositionLot

    positions = {
        "000001.XSHE": PositionLot(
            portfolio_id="strat1", symbol="000001.XSHE", quantity=1000,
            available_quantity=1000, avg_cost=12.50, trade_date=date.today(),
        ),
    }

    manifest = _frozen_manifest(package_id="pkg", manifest_sha256="aa")
    portfolio = PaperPortfolio(
        portfolio_id="strat1",
        portfolio_name="Production LocalSim",
        package_id="pkg",
        manifest_sha256="aa",
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.DB_HISTORICAL,
    )
    paper_repo = FakePaperRepository(portfolio, positions=positions, cash=999_000)

    def _pos_loader(strategy_id, trade_date):
        return positions

    provider = ProductionSimulationRunContextProvider(
        position_loader=_pos_loader,
        price_loader=lambda symbols, trade_date: {symbol: 12.6 for symbol in symbols},
        paper_repository_factory=lambda: paper_repo,
        enable_localsim_broker=False,
    )
    release = _make_test_release()
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)
    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())
    assert ctx.current_positions == positions
    assert ctx.portfolio_id == "strat1"
    assert ctx.current_prices == {"000001.XSHE": 12.6}
    assert ctx.cash == 999_000


def test_production_context_provider_fails_fast_on_position_failure():
    """Production provider must not turn a loader failure into empty positions."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    def failing_loader(strategy_id, trade_date):
        raise RuntimeError("db unreachable")

    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    portfolio = PaperPortfolio(
        portfolio_id="strat1",
        portfolio_name="Production LocalSim",
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.DB_HISTORICAL,
    )
    paper_repo = FakePaperRepository(portfolio, positions={}, cash=1_000_000)
    provider = ProductionSimulationRunContextProvider(
        position_loader=failing_loader,
        paper_repository_factory=lambda: paper_repo,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)
    with pytest.raises(DataUnavailableError, match="position_loader failed"):
        provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())


def test_production_context_provider_miniqmt_context():
    """Production provider wires MiniQMT services when backend is MINIQMT_SIM."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    positions = {
        "000001.XSHE": PositionLot(
            portfolio_id="strat1", symbol="000001.XSHE", quantity=1000,
            available_quantity=1000, avg_cost=12.50, trade_date=date.today(),
        ),
    }
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="strat1",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    provider = ProductionSimulationRunContextProvider(
        position_loader=lambda strategy_id, trade_date: positions,
        price_loader=lambda symbols, trade_date: {symbol: 12.6 for symbol in symbols},
        managed_order_service_factory=lambda: "fake_mos",
        qmt_sync_service_factory=lambda: "fake_sync",
        qmt_reconciliation_service_factory=lambda: "fake_recon",
        qmt_ledger_repository=qmt_repo,
    )
    release = _make_test_release()
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)
    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())
    assert ctx.current_positions == positions
    assert ctx.current_prices == {"000001.XSHE": 12.6}
    assert ctx.managed_order_service == "fake_mos"
    assert ctx.qmt_sync_service == "fake_sync"
    assert ctx.qmt_reconciliation_service == "fake_recon"
    assert ctx.qmt_ledger_repository is qmt_repo


def test_production_context_provider_loads_miniqmt_positions_from_virtual_ledger_without_submit_broker():
    """Default MiniQMT production context reads strategy lots and keeps submission disabled unless explicitly enabled."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="StrategyOne",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            frozen_cash=Decimal("123"),
            realized_pnl=Decimal("45"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_prod_context",
            strategy_id="strat1",
            symbol="000001.SZ",
            open_trade_id="trade_prod_context",
            open_date=TRADE_DATE,
            quantity=1000,
            available_quantity=1000,
            remaining_quantity=1000,
            avg_cost=Decimal("10.00"),
            cost_amount=Decimal("10000"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    qmt_client = FakeManagedOrderBroker(positions=[{"stock_code": "000001.SZ", "quantity": 1000, "can_sell": 1000}])
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 10.5 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: qmt_client,
        enable_miniqmt_submit=False,
    )
    release = _make_test_release()
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    assert ctx.current_positions["000001.SZ"].quantity == 1000
    assert ctx.current_prices == {"000001.SZ": 10.5}
    assert ctx.cash == 900000
    assert ctx.frozen_cash == 123
    assert ctx.realized_pnl == 45
    assert ctx.qmt_ledger_repository is qmt_repo
    assert ctx.qmt_sync_service is not None
    assert ctx.qmt_reconciliation_service is not None
    assert getattr(ctx.managed_order_service, "_broker") is qmt_client


def test_production_context_provider_miniqmt_preview_checks_broker_can_sell_without_submit():
    """Preview-only MiniQMT path can read account sellable quantity but never places orders."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="StrategyOne",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_preview_sell_check",
            strategy_id="strat1",
            symbol="000001.SZ",
            open_trade_id="trade_preview_sell_check",
            open_date=date(2026, 5, 20),
            quantity=1000,
            available_quantity=1000,
            remaining_quantity=1000,
            avg_cost=Decimal("10.00"),
            cost_amount=Decimal("10000"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker(positions=[{"stock_code": "000001.SZ", "quantity": 1000, "can_sell": 100}])
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 10.5 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        qmt_calendar_provider_factory=lambda: StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
        enable_miniqmt_submit=False,
    )
    release = _make_test_release()
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)
    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    result = ctx.managed_order_service.submit_batch([
        ManagedOrderRequest(
            account_id="QMT_SIM_ACCOUNT",
            strategy_name="StrategyOne",
            symbol="000001.SZ",
            side="SELL",
            order_type=24,
            quantity=200,
            price_type=5,
            price=Decimal("0"),
            order_remark="preview-sell-check",
            trade_date=TRADE_DATE,
            mode="SIM",
        )
    ])

    assert result.success is False
    assert result.results[0].broker_called is False
    assert result.results[0].preflight.broker_can_sell == 100
    assert result.results[0].preflight.primary_error.code in {"INSUFFICIENT_BROKER_CAN_SELL", "BATCH_INSUFFICIENT_BROKER_CAN_SELL"}
    assert broker.place_order_payloads == []


def test_production_context_provider_miniqmt_submit_defaults_to_preview_only_and_persists_ledger_evidence():
    """Production MiniQMT submit path must only write preview evidence unless submit is explicitly enabled."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_prod_preview_000003",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_prod_preview_000003",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
    snapshot_client = FakeQmtSnapshotClient(
        positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}]
    )
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: {"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0}[symbol] for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        qmt_calendar_provider_factory=lambda: StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
        qmt_sync_service_factory=lambda: QmtStrategyLedgerSyncService(
            repository=qmt_repo,
            qmt_client=snapshot_client,
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            trade_date=TRADE_DATE,
            calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
        ),
        qmt_reconciliation_service_factory=lambda: QmtStrategyLedgerReconciliationService(repository=qmt_repo),
        enable_miniqmt_submit=False,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=provider,
    )

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    restarted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    assert submitted.submitted_count == 1
    assert submitted.results[0].status == "RECONCILED"
    assert submitted.results[0].run.status == SimulationDailyRunStatus.SUCCEEDED
    payload = repo.get_simulation_daily_run(submitted.results[0].run.run_id).run_payload_json
    assert payload["broker_called"] is False
    assert payload["qmt_batch_status"] == "PREVIEW_SUCCEEDED"
    assert payload["qmt_batch_result"]["preview_only"] is True
    assert payload["qmt_batch_result"]["results"]
    assert all(result["broker_called"] is False for result in payload["qmt_batch_result"]["results"])
    batch = qmt_repo.get_order_batch(payload["qmt_batch_id"])
    assert batch is not None
    assert batch.metadata["preview_only"] is True
    assert batch.result_json["broker_called"] is False
    preview_intents = qmt_repo.list_order_intents_by_batch(payload["qmt_batch_id"])
    assert [intent.submit_status for intent in preview_intents] == [
        IntentSubmitStatus.CREATED,
        IntentSubmitStatus.CREATED,
        IntentSubmitStatus.CREATED,
    ]
    assert all(intent.metadata["preview_only"] is True for intent in preview_intents)
    assert broker.place_order_payloads == []
    assert restarted.results[0].status == "REUSED_EXISTING_PLAN"
    assert restarted.results[0].run.run_payload_json["qmt_batch_id"] == payload["qmt_batch_id"]
    assert restarted.results[0].run.run_payload_json["broker_called"] is False
    assert len(qmt_repo.list_order_intents_by_batch(payload["qmt_batch_id"])) == len(preview_intents)
    assert broker.place_order_payloads == []


def test_production_context_provider_builds_localsim_broker_from_persisted_paper_state():
    """LocalSim production context constructs a real broker using persisted Paper v2 cash and lots."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    portfolio = PaperPortfolio(
        portfolio_id="strat1",
        portfolio_name="LocalSim prod context",
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.DB_HISTORICAL,
    )
    positions = {
        "000001.SZ": PositionLot(
            portfolio_id="strat1",
            symbol="000001.SZ",
            quantity=1000,
            available_quantity=1000,
            avg_cost=10.0,
            trade_date=TRADE_DATE,
        )
    }
    paper_repo = FakePaperRepository(portfolio, positions=positions, cash=980_000)
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: paper_repo,
        price_loader=lambda symbols, trade_date: {symbol: 10.5 for symbol in symbols},
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    assert ctx.local_broker is not None
    assert ctx.local_broker.query_account().cash == Decimal("980000")
    assert ctx.local_broker.query_positions()["000001.SZ"].quantity == 1000


def test_scheduler_rejects_stale_selection_evidence_for_new_trade_date():
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    stale_evidence = _evidence(release, candidates=_candidate_rows())
    stale_selection = StrategyPackageSelectionResult(
        runtime_config={},
        package_results={release.package_id: _candidate_rows()},
        aggregate_results=_candidate_rows(),
        excluded_results={release.package_id: []},
        manifest_sha256_by_package={release.package_id: release.manifest_sha256},
        evidence_by_package={release.package_id: stale_evidence},
    )

    class StaleSelectionService:
        def run_selection(self, **kwargs):
            return stale_selection

    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=StaleSelectionService(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id=local_binding.strategy_id)}
        ),
    )

    result = scheduler.run_once(
        trade_date=date(2026, 5, 22),
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
    )

    assert result.failed_count == 1
    assert result.results[0].status == "FAILED"
    assert result.results[0].error["type"] == "DataUnavailableError"
    assert "stale daily selection evidence" in result.results[0].error["message"]


def test_scheduler_status_reports_provider_and_controlled_tick_capability():
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    scheduler = SimulationLifecycleScheduler(context_provider=ProductionSimulationRunContextProvider())
    status = scheduler.status()

    assert status["manual_tick_endpoint_enabled"] is True
    assert status["context_provider_mode"] == "production"
    assert status["context_provider"]["miniqmt_preview_enabled"] is True
    assert status["default_submit"] is False


def test_fail_fast_provider_still_rejects():
    """FailFastSimulationRunContextProvider still raises DataUnavailableError."""
    from backend.services.simulation_runtime.scheduler import FailFastSimulationRunContextProvider
    from backend.services.trading_core.errors import DataUnavailableError

    provider = FailFastSimulationRunContextProvider()
    release = _make_test_release()
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)
    try:
        provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())
        raise AssertionError('expected DataUnavailableError')
    except DataUnavailableError as exc:
        assert 'requires an explicit run context provider' in str(exc)


def _make_test_release():
    from backend.services.simulation_runtime.models import StrategyRuntimeRelease
    return StrategyRuntimeRelease(
        package_id="pkg", manifest_sha256="aa",
        runtime_profile_id="rp", runtime_profile_version_id="rpv", runtime_profile_sha256="rps",
        daily_strategy_profile_version_id="dsp", execution_policy_version_id="epv",
        execution_policy_sha256="eps", tail_policy_version_id="tpv", tail_policy_sha256="tps",
        release_config_json={
            "schema_version": "strategy_runtime_release_v1",
            "package_id": "pkg",
            "manifest_sha256": "aa",
            "runtime_profile": {"profile_id": "rp", "profile_version_id": "rpv", "config_sha256": "rps"},
            "daily_strategy": {"profile_version_id": "dsp"},
            "execution_policy": {"policy_version_id": "epv", "policy_sha256": "eps"},
            "tail_policy": {"policy_version_id": "tpv", "policy_sha256": "tps"},
            "validation_state": "DRAFT",
            "validation_evidence": {},
            "metadata": {},
        },
    )


def _make_test_binding(release, *, broker_backend):
    from backend.services.simulation_runtime.models import SimulationReleaseBinding
    return SimulationReleaseBinding(
        strategy_id="strat1", release_id=release.release_id, release_hash=release.release_hash or "",
        package_id=release.package_id, manifest_sha256=release.manifest_sha256,
        broker_backend=broker_backend, capital_allocation=1_000_000.0,
        binding_config_json={
            "schema_version": "simulation_release_binding_v1",
            "strategy_id": "strat1",
            "release_id": release.release_id,
            "release_hash": release.release_hash or "",
            "package_id": release.package_id,
            "manifest_sha256": release.manifest_sha256,
            "broker_backend": broker_backend.value,
            "capital_allocation": 1_000_000.0,
            "approval_state": "DRAFT",
            "metadata": {},
        },
    )
