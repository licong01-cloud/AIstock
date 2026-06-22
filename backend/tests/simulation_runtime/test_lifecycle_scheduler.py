from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from backend.services.paper_trading_v2.models import PaperPortfolio
from backend.services.paper_trading_v2.market_data import MinuteDataSource, MinuteExecutionMarketInput
from backend.services.paper_trading_v2.broker.localsim import LocalSimBackend
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.paper_trading_v2.broker.base import OrderHandle
from backend.services.qmt_strategy_ledger.lot_availability import StaticTradingCalendarProvider
from backend.services.qmt_strategy_ledger.models import (
    BUY_ORDER_TYPE,
    IntentSubmitStatus,
    OrderBatchStatus,
    OrderLedgerRecord,
    PositionLotRecord,
    SELL_ORDER_TYPE,
    STATUS_CANCELLED,
    STATUS_FILLED,
    STATUS_OPEN_LIKE,
    STATUS_PART_SUCC,
    STATUS_REJECTED,
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
    SimulationRuntimeOpsService,
    StaticSimulationRunContextProvider,
    StrategyPackageSelectionResult,
    StrategyPackageSelectionService,
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
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError
from backend.services.trading_core.models import MinuteBar, OrderIntent, OrderSide, OrderType, PositionLot


TRADE_DATE = date(2026, 5, 21)


def _release_and_bindings(*, qmt_only: bool = False, release_metadata: dict | None = None):
    repo = InMemorySimulationRuntimeRepository()
    service = StrategyRuntimeReleaseService(repository=repo)
    if qmt_only:
        execution_policy_version_id = "vnpy_asset:SNIPER_MINIQMT"
        execution_policy_sha256 = "exec_policy_hash_sniper_miniqmt"
        execution_policy_json = {"algo_code": "SNIPER_MINIQMT", "algo_config": {}}
    else:
        execution_policy_version_id = "exec_policy_v25_1_small_cap"
        execution_policy_sha256 = "exec_policy_hash_v25_1_small_cap"
        execution_policy_json = None
    release = service.create_release(
        package_id="pkg_scheduler",
        manifest_sha256="manifest_scheduler",
        runtime_profile_id="runtime_profile_scheduler",
        runtime_profile_version_id="runtime_profile_scheduler_v1",
        runtime_profile_sha256="runtime_profile_scheduler_hash",
        daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        execution_policy_version_id=execution_policy_version_id,
        execution_policy_sha256=execution_policy_sha256,
        execution_policy_json=execution_policy_json,
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


def _evidence(
    release,
    *,
    candidates: list[SelectionCandidate],
    valid_no_candidate: bool = False,
    target_trade_date: date = TRADE_DATE,
    cutoff_date: date = date(2026, 5, 20),
) -> DailySelectionEvidence:
    payload = {
        "schema_version": "daily_selection_evidence_v1",
        "target_trade_date": target_trade_date.isoformat(),
        "cutoff_date": cutoff_date.isoformat(),
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
        target_trade_date=target_trade_date,
        cutoff_date=cutoff_date,
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
        runtime_release = kwargs.get("runtime_release") or self.release
        target_trade_date = kwargs.get("trade_date") or TRADE_DATE
        evidence = _evidence(
            runtime_release,
            candidates=self.candidates,
            valid_no_candidate=self.valid_no_candidate,
            target_trade_date=target_trade_date,
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


def _position_context(*, portfolio_id: str, local_broker=None, cash: float | None = 100_000) -> SimulationRunContext:
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
        cash=cash,
    )


def _local_sim_execution_policy() -> dict[str, Any]:
    return {
        "policy_id": "exec_policy_twap",
        "policy_sha256": "exec_policy_hash_twap",
        "policy_json": {
            "algo_code": "TWAP",
            "algo_config": {
                "allow_partial_fill": True,
                "split_count": 1,
            },
        },
    }


def _local_sim_context_with_real_broker(
    *,
    portfolio_id: str,
    release: Any,
    cash: float = 100_000,
    positions: dict[str, PositionLot] | None = None,
    paper_repository: InMemoryPaperTradingV2Repository | None = None,
) -> SimulationRunContext:
    manifest = _score_weighted_manifest(release)
    current_positions = dict(positions or {})
    broker = LocalSimBackend(
        portfolio_id=portfolio_id,
        initial_cash=cash,
        initial_available_cash=cash,
        data_source=MinuteDataSource.DB_HISTORICAL,
        manifest=manifest,
        package_id=release.package_id,
        market_data_provider=FakeLocalSimMarketDataProvider(),
        execution_policy=_local_sim_execution_policy(),
        initial_positions=current_positions,
    )
    return SimulationRunContext(
        portfolio_id=portfolio_id,
        current_positions=current_positions,
        current_prices={
            symbol: 10.0
            for symbol in {"000001.SZ", "688001.SH", *current_positions}
        },
        top_k=1,
        execution_policy_payload=_local_sim_execution_policy(),
        local_broker=broker,
        paper_repository=paper_repository,
        cash=cash,
        market_data_source=MinuteDataSource.DB_HISTORICAL.value,
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
        self.positions = (
            list(positions)
            if positions is not None
            else [{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}]
        )
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


class FailingQmtSnapshotClient(FakeQmtSnapshotClient):
    def __init__(self, *, error_message: str = "broker snapshot unavailable", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.error_message = error_message
        self.fail = False

    def get_orders(self, cancelable_only: bool = False) -> list[dict[str, Any]]:
        if self.fail:
            raise RuntimeError(self.error_message)
        return super().get_orders(cancelable_only=cancelable_only)


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


class FakeLocalSimMarketDataProvider:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def load_symbol_input(
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
        min_bars: int,
        require_suspend_status: bool = False,
        require_day_features: bool = False,
    ) -> MinuteExecutionMarketInput:
        self.calls.append(
            {
                "symbol": symbol,
                "trade_date": trade_date,
                "source": source,
                "min_bars": min_bars,
                "require_suspend_status": require_suspend_status,
                "require_day_features": require_day_features,
            }
        )
        start = datetime.combine(trade_date, datetime.min.time()).replace(hour=9, minute=31)
        minute_bars = [
            MinuteBar(
                symbol=symbol,
                bar_time=start + timedelta(minutes=offset),
                open=10.0,
                high=10.2,
                low=9.9,
                close=10.1,
                volume=100_000,
                amount=1_000_000.0,
                limit_up=11.0,
                limit_down=9.0,
            )
            for offset in range(max(1, min_bars))
        ]
        return MinuteExecutionMarketInput(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            minute_bars=minute_bars,
            market_context={
                "stock_id": symbol,
                "trade_date": trade_date.isoformat(),
                "data_source": source.value,
                "prev_close": 10.0,
                "limit_up": 11.0,
                "limit_down": 9.0,
                "suspend_status": {"is_suspended": False},
            },
        )


class FakePreTradeTradabilityProvider:
    def __init__(self, statuses: dict[str, dict[str, Any]] | None = None) -> None:
        self.statuses = dict(statuses or {})
        self.calls: list[dict[str, Any]] = []

    def get_statuses(self, symbols: list[str], trade_date: date, *, require_realtime_quote: bool = False):
        self.calls.append(
            {
                "symbols": list(symbols),
                "trade_date": trade_date,
                "require_realtime_quote": require_realtime_quote,
            }
        )
        return {
            symbol: dict(
                self.statuses.get(
                    symbol,
                    {
                        "schema_version": "pre_trade_tradability_status_v1",
                        "symbol": symbol,
                        "trade_date": trade_date.isoformat(),
                        "is_tradable": True,
                        "reason_code": "OK",
                        "source": "unit_test",
                    },
                )
            )
            for symbol in symbols
        }


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


def _score_weighted_manifest(release, *, topk: int = 2, n_drop: int = 1) -> StrategyPackageManifest:
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    return manifest.model_copy(
        update={
            "backtest_context": {
                "daily_strategy": {
                    "strategy_id": "score_weighted_topk_v2",
                    "topk": topk,
                    "n_drop": n_drop,
                    "custom_params": {
                        "strategy_class": "score_weighted_topk_v2",
                        "topk": topk,
                        "n_drop": n_drop,
                        "max_n_drop": max(n_drop, 1),
                        "min_n_drop": 0,
                        "weight_method": "equal",
                        "max_position_ratio": 0.95,
                    },
                }
            },
            "manifest_sha256": release.manifest_sha256,
        }
    )


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


def test_scheduler_sizes_miniqmt_targets_from_dynamic_strategy_slot_equity() -> None:
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
            frozen_cash=Decimal("10000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    candidates = [
        SelectionCandidate(
            symbol="000001.SZ",
            score=0.99,
            rank=1,
            target_weight=0.10,
            reference_price=10.0,
            reason="daily_strategy_buy_or_retain",
        )
    ]
    context = SimulationRunContext(
        portfolio_id=qmt_binding.strategy_id,
        current_positions={
            "000003.SZ": PositionLot(
                portfolio_id=qmt_binding.strategy_id,
                symbol="000003.SZ",
                quantity=1000,
                available_quantity=1000,
                avg_cost=9.0,
                trade_date=date(2026, 5, 20),
            )
        },
        current_prices={"000003.SZ": 20.0},
        qmt_ledger_repository=qmt_repo,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=candidates),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={qmt_binding.binding_id: context}),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )

    assert result.planned_count == 1
    plan = result.results[0].execution_plan
    assert plan is not None
    buy = next(intent for intent in plan.intents if intent.symbol == "000001.SZ")
    assert buy.order_quantity == 1300
    run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    basis = run.run_payload_json["target_equity_basis"]
    assert basis["source"] == "miniqmt_strategy_slot_dynamic_equity"
    assert basis["cash"] == 100_000.0
    assert basis["frozen_cash"] == 10_000.0
    assert basis["market_value"] == 20_000.0
    assert basis["total_equity"] == 130_000.0
    assert basis["capital_allocation"] == 100_000.0


def test_scheduler_persists_no_rebalance_evidence_when_targets_match_current_positions() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id=qmt_binding.strategy_id,
                    current_positions={
                        "000001.SZ": PositionLot(
                            portfolio_id=qmt_binding.strategy_id,
                            symbol="000001.SZ",
                            quantity=1000,
                            available_quantity=1000,
                            avg_cost=10.0,
                            trade_date=date(2026, 5, 20),
                        ),
                        "688001.SH": PositionLot(
                            portfolio_id=qmt_binding.strategy_id,
                            symbol="688001.SH",
                            quantity=201,
                            available_quantity=201,
                            avg_cost=20.0,
                            trade_date=date(2026, 5, 20),
                        ),
                    },
                    current_prices={"000001.SZ": 10.0, "688001.SH": 20.0},
                )
            }
        ),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )

    assert result.planned_count == 1
    run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    evidence = run.run_payload_json["no_rebalance_evidence"]
    assert evidence["reason_code"] == "TOP_LIST_AND_QUANTITY_MATCH"
    assert evidence["selected_symbols"] == ["000001.SZ", "688001.SH"]
    assert evidence["target_symbols"] == ["000001.SZ", "688001.SH"]
    assert all(row["delta_quantity"] == 0 for row in evidence["rows"])


def test_scheduler_rolls_forward_expired_localsim_binding_for_unattended_daily_runs() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    prepared_day = TRADE_DATE
    next_trade_day = TRADE_DATE + timedelta(days=1)
    local_binding = local_binding.model_copy(update={"effective_from": prepared_day, "effective_to": prepared_day})
    repo.bindings[local_binding.binding_id] = local_binding
    fake_selection = FakeSelectionService(release, candidates=_candidate_rows())
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=fake_selection,
        context_provider=StaticSimulationRunContextProvider(
            by_strategy_id={local_binding.strategy_id: _position_context(portfolio_id="portfolio_roll_forward")}
        ),
    )

    result = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    rerun = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )

    assert result.total_bindings == 1
    assert result.planned_count == 1
    rolled_binding = result.results[0].run
    assert rolled_binding is not None
    assert rolled_binding.binding_id != local_binding.binding_id
    new_binding = repo.get_simulation_release_binding(rolled_binding.binding_id)
    assert new_binding.strategy_id == local_binding.strategy_id
    assert new_binding.effective_from == next_trade_day
    assert new_binding.effective_to == next_trade_day
    assert new_binding.binding_config_json["metadata"]["purpose"] == "localsim_unattended_daily_roll_forward"
    assert new_binding.binding_config_json["metadata"]["extends_binding_id"] == local_binding.binding_id
    new_release = repo.get_strategy_runtime_release(new_binding.release_id)
    assert new_release.base_release_id == release.release_id
    assert new_release.effective_from == next_trade_day
    assert new_release.effective_to == next_trade_day
    assert rerun.reused_count == 1
    assert rerun.results[0].run.binding_id == new_binding.binding_id
    local_bindings = repo.list_simulation_release_bindings(
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        approval_states=[SimulationBindingApprovalState.SIM_VALIDATING],
        limit=10,
    )
    assert len([binding for binding in local_bindings if binding.effective_from == next_trade_day]) == 1


def test_scheduler_rolls_forward_expired_miniqmt_binding_for_unattended_daily_runs() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    prepared_day = TRADE_DATE
    next_trade_day = TRADE_DATE + timedelta(days=1)
    qmt_binding = qmt_binding.model_copy(update={"effective_from": prepared_day, "effective_to": prepared_day})
    repo.bindings[qmt_binding.binding_id] = qmt_binding
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_strategy_id={qmt_binding.strategy_id: _position_context(portfolio_id="portfolio_miniqmt_roll_forward")}
        ),
    )

    result = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    rerun = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )

    assert result.total_bindings == 1
    assert result.planned_count == 1
    rolled_run = result.results[0].run
    assert rolled_run is not None
    assert rolled_run.binding_id != qmt_binding.binding_id
    assert rolled_run.account_group_id == qmt_binding.account_group_id
    assert rolled_run.strategy_slot_id == qmt_binding.strategy_slot_id
    new_binding = repo.get_simulation_release_binding(rolled_run.binding_id)
    assert new_binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
    assert new_binding.account_group_id == qmt_binding.account_group_id
    assert new_binding.strategy_slot_id == qmt_binding.strategy_slot_id
    assert new_binding.strategy_name == qmt_binding.strategy_name
    assert new_binding.order_remark_prefix == qmt_binding.order_remark_prefix
    assert new_binding.effective_from == next_trade_day
    assert new_binding.effective_to == next_trade_day
    assert new_binding.binding_config_json["metadata"]["purpose"] == "miniqmt_unattended_daily_roll_forward"
    assert new_binding.binding_config_json["metadata"]["extends_binding_id"] == qmt_binding.binding_id
    new_release = repo.get_strategy_runtime_release(new_binding.release_id)
    assert new_release.base_release_id == release.release_id
    assert new_release.effective_from == next_trade_day
    assert new_release.effective_to == next_trade_day
    assert new_release.release_config_json["metadata"]["purpose"] == "miniqmt_unattended_daily_roll_forward"
    assert rerun.reused_count == 1
    assert rerun.results[0].run.binding_id == new_binding.binding_id


def test_scheduler_rolls_forward_local_and_miniqmt_when_backend_filter_is_omitted() -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings()
    assert local_binding is not None
    prepared_day = TRADE_DATE
    next_trade_day = TRADE_DATE + timedelta(days=1)
    local_binding = local_binding.model_copy(update={"effective_from": prepared_day, "effective_to": prepared_day})
    qmt_binding = qmt_binding.model_copy(update={"effective_from": prepared_day, "effective_to": prepared_day})
    repo.bindings[local_binding.binding_id] = local_binding
    repo.bindings[qmt_binding.binding_id] = qmt_binding
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_strategy_id={
                local_binding.strategy_id: _position_context(portfolio_id="portfolio_local_roll_all"),
                qmt_binding.strategy_id: _position_context(portfolio_id="portfolio_miniqmt_roll_all"),
            }
        ),
    )

    result = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        submit=False,
    )

    assert result.total_bindings == 2
    assert result.planned_count == 2
    assert {item.broker_backend for item in result.results} == {
        SimulationBrokerBackend.LOCAL_SIM,
        SimulationBrokerBackend.MINIQMT_SIM,
    }
    rolled_bindings = [repo.get_simulation_release_binding(item.run.binding_id) for item in result.results]
    assert {binding.binding_config_json["metadata"]["purpose"] for binding in rolled_bindings} == {
        "localsim_unattended_daily_roll_forward",
        "miniqmt_unattended_daily_roll_forward",
    }


def test_scheduler_rolls_forward_new_localsim_strategy_without_manual_next_day_binding() -> None:
    release, local_binding_a, _, repo = _release_and_bindings()
    assert local_binding_a is not None
    local_binding_b = _create_extra_binding(
        release=release,
        repo=repo,
        strategy_id="strategy_new_localsim_package",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
    )
    prepared_day = TRADE_DATE
    next_trade_day = TRADE_DATE + timedelta(days=1)
    for binding in (local_binding_a, local_binding_b):
        expired = binding.model_copy(update={"effective_from": prepared_day, "effective_to": prepared_day})
        repo.bindings[expired.binding_id] = expired
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_strategy_id={
                local_binding_a.strategy_id: _position_context(portfolio_id="portfolio_roll_a"),
                local_binding_b.strategy_id: _position_context(portfolio_id="portfolio_roll_b"),
            }
        ),
    )

    result = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )

    assert result.total_bindings == 2
    assert result.planned_count == 2
    assert {item.strategy_id for item in result.results} == {
        local_binding_a.strategy_id,
        local_binding_b.strategy_id,
    }
    assert all(item.run.binding_id not in {local_binding_a.binding_id, local_binding_b.binding_id} for item in result.results)


def test_scheduler_roll_forward_keeps_active_binding_when_limit_is_full() -> None:
    _, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    next_trade_day = TRADE_DATE + timedelta(days=1)
    active_binding = local_binding.model_copy(update={"effective_from": next_trade_day, "effective_to": next_trade_day})
    repo.bindings[active_binding.binding_id] = active_binding
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(repo.get_strategy_runtime_release(active_binding.release_id), candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_strategy_id={active_binding.strategy_id: _position_context(portfolio_id="portfolio_limit_full")}
        ),
    )

    result = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        limit=1,
    )

    assert result.total_bindings == 1
    assert result.results[0].binding_id == active_binding.binding_id
    assert result.planned_count == 1
    assert len(repo.bindings) == 2


def test_repository_latest_binding_ignores_future_manual_binding_for_roll_forward() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    prepared_day = TRADE_DATE
    next_trade_day = TRADE_DATE + timedelta(days=1)
    future_day = next_trade_day + timedelta(days=3)
    expired = local_binding.model_copy(update={"effective_from": prepared_day, "effective_to": prepared_day})
    future = _create_extra_binding(
        release=release,
        repo=repo,
        strategy_id=local_binding.strategy_id,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        strategy_name="Future LocalSim 2026-05-25",
    ).model_copy(update={"effective_from": future_day, "effective_to": future_day})
    repo.bindings[expired.binding_id] = expired
    repo.bindings[future.binding_id] = future
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_strategy_id={local_binding.strategy_id: _position_context(portfolio_id="portfolio_future_binding")}
        ),
    )

    result = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )

    assert result.total_bindings == 1
    assert result.planned_count == 1
    assert result.results[0].run is not None
    assert result.results[0].run.binding_id not in {expired.binding_id, future.binding_id}
    rolled = repo.get_simulation_release_binding(result.results[0].run.binding_id)
    assert rolled.effective_from == next_trade_day
    assert rolled.binding_config_json["metadata"]["extends_binding_id"] == expired.binding_id


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
    paper_repo = InMemoryPaperTradingV2Repository()
    context_provider = CountingContextProvider(
        by_binding_id={
            local_binding.binding_id: _local_sim_context_with_real_broker(
                portfolio_id="portfolio_shared",
                release=release,
                paper_repository=paper_repo,
            )
        }
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
    run_id = first.results[0].run.run_id
    persisted_order_count = len(paper_repo.list_orders_for_run(run_id))
    assert persisted_order_count == len(first.results[0].execution_plan.intents)
    assert paper_repo.list_fills_for_run(run_id)
    assert paper_repo.cash_entries[run_id]
    payload = first.results[0].run.run_payload_json
    assert payload["local_sim_persistence"]["status"] == "PERSISTED"
    assert payload["local_sim_persistence"]["fill_count"] == len(paper_repo.list_fills_for_run(run_id))
    assert payload["strategy_performance"]["cash"] < 100_000
    assert payload["strategy_performance"]["positions"]
    run_events = paper_repo.list_run_events("portfolio_shared", run_id=run_id)
    success_event = next(event for event in run_events if event["event_type"] == "RUN_SUCCEEDED")
    assert success_event["context"]["source"] == "simulation_runtime_local_sim"
    assert success_event["context"]["simulation_run_id"] == run_id
    assert success_event["context"]["fill_count"] == len(paper_repo.list_fills_for_run(run_id))

    restarted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    assert restarted.reused_count == 1
    assert len(fake_selection.calls) == 1
    assert context_provider.calls == [local_binding.binding_id]
    assert len(paper_repo.list_orders_for_run(run_id)) == persisted_order_count
    assert restarted.results[0].execution_plan.plan_id == first.results[0].execution_plan.plan_id


def test_scheduler_submits_existing_local_plan_after_restart_when_broker_was_not_called() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    fake_selection = FakeSelectionService(release, candidates=_candidate_rows())
    plan_only_context = StaticSimulationRunContextProvider(
        by_binding_id={
            local_binding.binding_id: _local_sim_context_with_real_broker(
                portfolio_id="portfolio_shared",
                release=release,
            )
        }
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
    paper_repo = InMemoryPaperTradingV2Repository()
    scheduler.context_provider = CountingContextProvider(
        by_binding_id={
            local_binding.binding_id: _local_sim_context_with_real_broker(
                portfolio_id="portfolio_shared",
                release=release,
                paper_repository=paper_repo,
            )
        }
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
    run_id = restarted.results[0].run.run_id
    assert len(paper_repo.list_orders_for_run(run_id)) == len(planned.results[0].execution_plan.intents)
    assert paper_repo.list_fills_for_run(run_id)
    assert paper_repo.cash_entries[run_id]
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
            by_binding_id={
                local_binding.binding_id: _local_sim_context_with_real_broker(
                    portfolio_id="portfolio_shared",
                    release=release,
                )
            }
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

    paper_repo = InMemoryPaperTradingV2Repository()
    scheduler.context_provider = CountingContextProvider(
        by_binding_id={
            local_binding.binding_id: _local_sim_context_with_real_broker(
                portfolio_id="portfolio_shared",
                release=release,
                paper_repository=paper_repo,
            )
        }
    )
    recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    assert recovered.results[0].status == "SUBMITTED"
    assert recovered.results[0].run.status == SimulationDailyRunStatus.SUCCEEDED
    run_id = recovered.results[0].run.run_id
    assert len(paper_repo.list_orders_for_run(run_id)) == len(planned.results[0].execution_plan.intents)
    assert paper_repo.list_fills_for_run(run_id)
    assert paper_repo.cash_entries[run_id]
    assert recovered.results[0].run.run_payload_json["broker_called"] is True
    assert recovered.results[0].run.run_payload_json["last_stage"] == "SUCCEEDED"


def test_scheduler_fails_localsim_submit_without_durable_execution_snapshot() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _position_context(
                    portfolio_id="portfolio_shared",
                    local_broker=FakeLocalSimBroker(),
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
    runs = repo.list_simulation_daily_runs(
        trade_date=TRADE_DATE,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        limit=10,
    )

    assert result.failed_count == 1
    assert result.results[0].error["context"]["run_id"] == runs[0].run_id
    assert runs[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert runs[0].run_payload_json["broker_called"] is True
    assert "local_sim_persistence" not in runs[0].run_payload_json
    assert runs[0].run_payload_json["submit_failure"]["stage"] == "LOCAL_SIM_PERSISTENCE_SNAPSHOT_MISSING"


def test_scheduler_fails_closed_when_localsim_cash_context_is_missing() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    broker = FakeLocalSimBroker()
    context = SimulationRunContext(
        portfolio_id="portfolio_missing_cash",
        current_positions={},
        current_prices={"000001.SZ": 10.0, "688001.SH": 20.0},
        local_broker=broker,
        top_k=1,
        cash=None,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: context}),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )
    latest_run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    diagnostic = latest_run.run_payload_json["local_sim_retry_diagnostics"]

    assert result.failed_count == 1
    assert result.results[0].error["context"]["reason_code"] == "LOCALSIM_CASH_CONTEXT_MISSING"
    assert latest_run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert diagnostic["stage"] == "LOCAL_SIM_CASH_CONTEXT_MISSING"
    assert diagnostic["context"]["reason_code"] == "LOCALSIM_CASH_CONTEXT_MISSING"
    assert latest_run.run_payload_json["broker_called"] is False
    assert broker.submitted == []


def test_scheduler_localsim_cash_fit_runs_sells_before_buys_and_skips_cash_residual() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    positions = {
        "000003.SZ": PositionLot(
            portfolio_id="portfolio_cash_fit",
            symbol="000003.SZ",
            quantity=1200,
            available_quantity=1200,
            avg_cost=10.0,
            trade_date=TRADE_DATE - timedelta(days=1),
        )
    }
    paper_repo = InMemoryPaperTradingV2Repository()
    context = _local_sim_context_with_real_broker(
        portfolio_id="portfolio_cash_fit",
        release=release,
        cash=50.0,
        positions=positions,
        paper_repository=paper_repo,
    )
    context = replace(context, top_k=2)
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: context}),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    latest_run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    payload = latest_run.run_payload_json["local_sim_cash_fit"]
    submitted = result.results[0].execution_plan.intents
    assert result.results[0].status == "LOCALSIM_CAPACITY_RESIDUAL_TERMINAL"
    assert latest_run.status == SimulationDailyRunStatus.FAILED_TERMINAL
    assert latest_run.run_payload_json["last_stage"] == "FAILED_TERMINAL"
    assert latest_run.run_payload_json["local_sim_persistence"]["status"] == "PERSISTED_WITH_CAPACITY_RESIDUAL"
    assert payload["status"] == "CAPACITY_RESIDUAL_SKIPPED"
    assert payload["sell_intent_count"] == 1
    assert payload["submitted_buy_count"] == 1
    assert payload["skipped_buy_count"] == 1
    assert [intent.side for intent in submitted] == [OrderSide.SELL, OrderSide.BUY]
    assert submitted[0].symbol == "000003.SZ"
    assert paper_repo.list_fills_for_run(latest_run.run_id)
    assert "000003.SZ" not in context.local_broker.query_positions()


def test_scheduler_rebuilds_localsim_insufficient_cash_failure_with_fresh_context() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    fake_selection = FakeSelectionService(release, candidates=_candidate_rows())
    initial_context = _local_sim_context_with_real_broker(
        portfolio_id="portfolio_rebuild_cash_fit",
        release=release,
        cash=50.0,
        positions={
            "000003.SZ": PositionLot(
                portfolio_id="portfolio_rebuild_cash_fit",
                symbol="000003.SZ",
                quantity=1200,
                available_quantity=0,
                avg_cost=10.0,
                trade_date=TRADE_DATE,
            )
        },
    )
    initial_context = replace(initial_context, top_k=2)
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=fake_selection,
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: initial_context}),
    )

    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    planned_run = planned.results[0].run
    failed_run = repo.update_simulation_daily_run(
        planned_run.run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        payload_patch={
            "last_stage": "FAILED_RETRYABLE",
            "submit_failure": {
                "stage": "LOCAL_SIM_SUBMIT_FAILED",
                "type": "BrokerRejectedError",
                "message": "LocalSim ledger rejected the order",
                "context": {"cause": "insufficient cash for buy fill", "cause_code": "RISK_RULE_ERROR"},
            },
            "broker_called": False,
        },
    )

    paper_repo = InMemoryPaperTradingV2Repository()
    recovered_context = _local_sim_context_with_real_broker(
        portfolio_id="portfolio_rebuild_cash_fit",
        release=release,
        cash=50.0,
        positions={
            "000003.SZ": PositionLot(
                portfolio_id="portfolio_rebuild_cash_fit",
                symbol="000003.SZ",
                quantity=1200,
                available_quantity=1200,
                avg_cost=10.0,
                trade_date=TRADE_DATE - timedelta(days=1),
            )
        },
        paper_repository=paper_repo,
    )
    recovered_context = replace(recovered_context, top_k=2)
    scheduler.context_provider = StaticSimulationRunContextProvider(
        by_binding_id={local_binding.binding_id: recovered_context}
    )

    recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    latest_run = repo.get_simulation_daily_run(failed_run.run_id)
    assert recovered.results[0].status == "LOCALSIM_CAPACITY_RESIDUAL_TERMINAL"
    assert latest_run.status == SimulationDailyRunStatus.FAILED_TERMINAL
    assert latest_run.execution_plan_id != failed_run.execution_plan_id
    assert latest_run.run_payload_json["rebuilt_failure_backend"] == SimulationBrokerBackend.LOCAL_SIM.value
    assert latest_run.run_payload_json["local_sim_cash_fit"]["status"] == "CAPACITY_RESIDUAL_SKIPPED"
    assert latest_run.run_payload_json["local_sim_persistence"]["status"] == "PERSISTED_WITH_CAPACITY_RESIDUAL"
    assert len(fake_selection.calls) == 2
    assert paper_repo.list_fills_for_run(latest_run.run_id)


def test_scheduler_marks_localsim_buy_only_retry_failure_with_actionable_context() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_buy_only_retry",
                    current_positions={},
                    current_prices={"000001.SZ": 10.0, "688001.SH": 20.0},
                )
            }
        ),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    planned_run = planned.results[0].run
    assert planned_run is not None
    plan = planned.results[0].execution_plan
    assert plan is not None
    assert all(intent.side == OrderSide.BUY for intent in plan.intents)
    failed_run = repo.update_simulation_daily_run(
        planned_run.run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        payload_patch={
            "last_stage": "FAILED_RETRYABLE",
            "broker_called": False,
            "no_rebalance_required": False,
        },
    )

    class FailingLocalSimContextProvider:
        def load_context(self, *, runtime_release, binding, trade_date):
            raise DataUnavailableError(
                "LocalSim could not load minute market data",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "trade_date": trade_date.isoformat(),
                    "source": "TDX_REALTIME",
                },
            )

    scheduler.context_provider = FailingLocalSimContextProvider()

    retried = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    latest_run = repo.get_simulation_daily_run(failed_run.run_id)
    assert retried.failed_count == 1
    assert retried.results[0].error["context"]["stage"] == "LOCAL_SIM_MARKET_DATA_UNAVAILABLE"
    assert latest_run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert latest_run.run_payload_json["broker_called"] is False
    assert latest_run.run_payload_json["failed_intents"] == len(plan.intents)
    diagnostics = latest_run.run_payload_json["local_sim_retry_diagnostics"]
    assert diagnostics["buy_intent_count"] == len(plan.intents)
    assert diagnostics["sell_intent_count"] == 0
    assert diagnostics["next_action"]

    detail = SimulationRuntimeOpsService(repository=repo).get_run_detail(latest_run.run_id)
    assert detail["run"]["broker_context"]["local_sim_retry_diagnostics"]["plan_id"] == plan.plan_id
    assert detail["run"]["errors"][0]["source"] == "local_sim_submit_failure"
    assert detail["run"]["errors"][0]["code"] == "LOCAL_SIM_MARKET_DATA_UNAVAILABLE"


def test_scheduler_clears_localsim_retry_diagnostics_after_successful_retry() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    candidates = _candidate_rows()[:1]
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=candidates),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_retry_clears_diagnostics",
                    current_positions={},
                    current_prices={"000001.SZ": 10.0, "688001.SH": 20.0},
                    top_k=1,
                )
            }
        ),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    planned_run = planned.results[0].run
    assert planned_run is not None
    plan = planned.results[0].execution_plan
    assert plan is not None
    failed_run = repo.update_simulation_daily_run(
        planned_run.run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        payload_patch={
            "last_stage": "FAILED_RETRYABLE",
            "broker_called": False,
            "no_rebalance_required": False,
        },
    )

    class FailingLocalSimContextProvider:
        def load_context(self, *, runtime_release, binding, trade_date):
            raise DataUnavailableError(
                "LocalSim could not load minute market data",
                context={"trade_date": trade_date.isoformat()},
            )

    scheduler.context_provider = FailingLocalSimContextProvider()
    failed_retry = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )
    assert failed_retry.failed_count == 1
    retry_payload = repo.get_simulation_daily_run(failed_run.run_id).run_payload_json
    assert retry_payload["local_sim_retry_diagnostics"]["stage"] == "LOCAL_SIM_MARKET_DATA_UNAVAILABLE"

    paper_repo = InMemoryPaperTradingV2Repository()
    scheduler.context_provider = StaticSimulationRunContextProvider(
        by_binding_id={
            local_binding.binding_id: _local_sim_context_with_real_broker(
                portfolio_id="portfolio_retry_clears_diagnostics",
                release=release,
                paper_repository=paper_repo,
            )
        }
    )
    recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    latest_run = repo.get_simulation_daily_run(failed_run.run_id)
    assert recovered.results[0].status == "SUBMITTED"
    assert latest_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert latest_run.execution_plan_id == plan.plan_id
    assert "submit_failure" not in latest_run.run_payload_json
    assert "local_sim_retry_diagnostics" not in latest_run.run_payload_json
    assert paper_repo.list_fills_for_run(latest_run.run_id)


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
    assert {
        position["symbol"]
        for position in latest_run.run_payload_json["strategy_performance"]["positions"]
    } >= {"000003.SZ"}
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


def test_scheduler_miniqmt_preflight_failure_stays_retryable_and_can_resubmit() -> None:
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
            cash=Decimal("1"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_000003_preflight_retry",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_000003_preflight_retry",
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

    failed = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    failed_run = repo.get_simulation_daily_run(failed.results[0].run.run_id)

    assert failed.results[0].status == "BROKER_SUBMIT_FAILED_RECONCILED"
    assert failed_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert failed_run.run_payload_json["qmt_batch_status"] == OrderBatchStatus.PARTIAL.value
    assert failed_run.run_payload_json["broker_called"] is True
    assert failed_run.run_payload_json["submitted_intents"] == 1
    assert failed_run.run_payload_json["failed_intents"] == 1
    assert failed_run.run_payload_json["qmt_batch_result"]["results"][1]["preflight"]["primary_error_code"] == "SKIPPED_INSUFFICIENT_CAPITAL"
    reconciliation = failed_run.run_payload_json["reconcile_after_submit"]
    assert reconciliation["submit_result_gate"]["status"] == "SUCCEEDED"
    assert reconciliation["submit_result_gate"]["reason"] == "miniqmt_capacity_residual_skipped_and_reconciled"
    assert reconciliation["qmt_batch_residual_summary"]["capacity_residual_count"] == 1
    assert [payload["order_type"] for payload in broker.place_order_payloads] == [SELL_ORDER_TYPE]

    account = qmt_repo.get_virtual_account(qmt_binding.strategy_id)
    qmt_repo.update_virtual_account(replace(account, cash=Decimal("100000")))
    recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    recovered_run = repo.get_simulation_daily_run(failed_run.run_id)

    assert recovered.results[0].status == "REUSED_EXISTING_PLAN"
    assert recovered_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert recovered_run.run_payload_json["qmt_batch_status"] == OrderBatchStatus.PARTIAL.value
    assert len(broker.place_order_payloads) == 1
    assert [payload["order_type"] for payload in broker.place_order_payloads] == [SELL_ORDER_TYPE]


def test_scheduler_keeps_miniqmt_capacity_residual_pending_when_open_orders_remain() -> None:
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
            cash=Decimal("1"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_open_order_capacity",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_open_order_capacity",
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

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    accepted_intent = next(
        intent for intent in qmt_repo.list_order_intents_by_batch(run.run_payload_json["qmt_batch_id"])
        if intent.submit_status == IntentSubmitStatus.ACCEPTED
    )
    qmt_repo.upsert_order_ledger(
        OrderLedgerRecord(
            intent_id=accepted_intent.intent_id,
            strategy_id=accepted_intent.strategy_id,
            strategy_name=accepted_intent.strategy_name,
            qmt_order_id="900000001",
            symbol=accepted_intent.symbol,
            order_type=accepted_intent.order_type,
            order_volume=accepted_intent.quantity,
            traded_volume=max(int(accepted_intent.quantity) - 20, 0),
            order_status=STATUS_PART_SUCC,
            account_id=accepted_intent.account_id,
            trade_date=accepted_intent.trade_date,
            price_type=accepted_intent.price_type,
            price=Decimal("8.0"),
            status_msg="partially filled but still open at close",
            order_remark=accepted_intent.order_remark,
        )
    )
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.SUBMITTING)

    reconciled = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    latest = repo.get_simulation_daily_run(run.run_id)
    reconciliation = latest.run_payload_json["reconcile_after_submit"]

    assert reconciled.results[0].status == "RECONCILIATION_PENDING_OPEN_ORDERS"
    assert latest.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert reconciliation["submit_result_gate"]["status"] == "PENDING"
    assert reconciliation["submit_result_gate"]["reason"] == "miniqmt_open_orders_pending_after_reconciliation"
    assert reconciliation["submit_result_gate"]["pending_open_orders"] is True
    assert reconciliation["open_order_evidence"]["open_order_count"] == 1
    assert reconciliation["open_order_evidence"]["open_orders"][0]["order_status"] == STATUS_PART_SUCC
    assert len(broker.place_order_payloads) == 1

    polled = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    still_pending = repo.get_simulation_daily_run(run.run_id)

    assert polled.results[0].status == "RECONCILIATION_PENDING_OPEN_ORDERS"
    assert still_pending.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert len(broker.place_order_payloads) == 1


def test_scheduler_miniqmt_open_order_evidence_excludes_terminal_xtquant_statuses() -> None:
    _release, _, qmt_binding, _repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    for status in (STATUS_CANCELLED, STATUS_FILLED, STATUS_REJECTED):
        qmt_repo.upsert_order_ledger(
            OrderLedgerRecord(
                intent_id=f"intent_terminal_{status}",
                strategy_id=qmt_binding.strategy_id,
                strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
                qmt_order_id=f"terminal_{status}",
                symbol="000003.SZ",
                order_type=SELL_ORDER_TYPE,
                order_volume=100,
                traded_volume=50 if status != STATUS_FILLED else 100,
                order_status=status,
                account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                trade_date=TRADE_DATE,
                status_msg=f"terminal xtquant status {status}",
                order_remark=f"remark_terminal_{status}",
            )
        )

    evidence = SimulationLifecycleScheduler._miniqmt_open_order_evidence(
        binding=qmt_binding,
        run=SimpleNamespace(trade_date=TRADE_DATE, run_payload_json={}),
        context=SimulationRunContext(current_positions={}, qmt_ledger_repository=qmt_repo),
    )

    assert evidence["open_order_count"] == 0
    assert evidence["open_orders"] == []


def test_scheduler_post_close_terminalizes_miniqmt_capacity_residual_without_fake_success() -> None:
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
            cash=Decimal("1"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_post_close_capacity",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_post_close_capacity",
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

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)
    post_close = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 15, 5),
    )
    latest = repo.get_simulation_daily_run(run.run_id)

    assert post_close.stale_terminalized_count == 1
    assert post_close.total_bindings == 1
    assert post_close.results[0].status == "POST_CLOSE_TERMINALIZED"
    assert latest.status == SimulationDailyRunStatus.SUCCEEDED
    terminalization = latest.run_payload_json["miniqmt_post_close_terminalization"]
    assert terminalization["audit_state"] == "succeeded_with_capacity_residual"
    assert terminalization["reason"] == "miniqmt_post_close_capacity_residual_skipped"
    assert terminalization["residual_summary"]["capacity_residual_count"] == 1
    assert latest.run_payload_json["qmt_batch_status"] == OrderBatchStatus.PARTIAL.value
    assert len(broker.place_order_payloads) == 1


def test_scheduler_post_close_terminalizes_miniqmt_open_orders_as_failed_terminal() -> None:
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
            cash=Decimal("1"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_post_close_open_order",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_post_close_open_order",
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

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    accepted_intent = next(
        intent for intent in qmt_repo.list_order_intents_by_batch(run.run_payload_json["qmt_batch_id"])
        if intent.submit_status == IntentSubmitStatus.ACCEPTED
    )
    qmt_repo.upsert_order_ledger(
        OrderLedgerRecord(
            intent_id=accepted_intent.intent_id,
            strategy_id=accepted_intent.strategy_id,
            strategy_name=accepted_intent.strategy_name,
            qmt_order_id="900000002",
            symbol=accepted_intent.symbol,
            order_type=accepted_intent.order_type,
            order_volume=accepted_intent.quantity,
            traded_volume=0,
            order_status=STATUS_OPEN_LIKE,
            account_id=accepted_intent.account_id,
            trade_date=accepted_intent.trade_date,
            price_type=accepted_intent.price_type,
            price=Decimal("8.0"),
            status_msg="accepted but still open at close",
            order_remark=accepted_intent.order_remark,
        )
    )
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)
    reconciled = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    pending = repo.get_simulation_daily_run(run.run_id)
    assert reconciled.results[0].status == "RECONCILIATION_PENDING_OPEN_ORDERS"
    assert pending.status == SimulationDailyRunStatus.INTRADAY_RUNNING

    post_close = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 15, 5),
    )
    latest = repo.get_simulation_daily_run(run.run_id)

    assert post_close.stale_terminalized_count == 1
    assert post_close.results[0].status == "POST_CLOSE_TERMINALIZED"
    assert latest.status == SimulationDailyRunStatus.FAILED_TERMINAL
    terminalization = latest.run_payload_json["miniqmt_post_close_terminalization"]
    assert terminalization["audit_state"] == "failed_terminal_after_close"
    assert terminalization["reason"] == "miniqmt_post_close_open_orders_terminal_failed"
    assert terminalization["open_order_evidence"]["open_order_count"] == 1
    assert len(broker.place_order_payloads) == 1


def test_scheduler_post_close_reconciles_fresh_broker_before_terminal_status() -> None:
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
            lot_id="lot_scheduler_qmt_post_close_filled",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_post_close_filled",
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

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    accepted_intent = next(
        intent
        for intent in qmt_repo.list_order_intents_by_batch(run.run_payload_json["qmt_batch_id"])
        if intent.submit_status == IntentSubmitStatus.ACCEPTED
    )
    open_order_id = "900000003"
    qmt_repo.upsert_order_ledger(
        OrderLedgerRecord(
            intent_id=accepted_intent.intent_id,
            strategy_id=accepted_intent.strategy_id,
            strategy_name=accepted_intent.strategy_name,
            qmt_order_id=open_order_id,
            symbol=accepted_intent.symbol,
            order_type=accepted_intent.order_type,
            order_volume=accepted_intent.quantity,
            traded_volume=0,
            order_status=STATUS_OPEN_LIKE,
            account_id=accepted_intent.account_id,
            trade_date=accepted_intent.trade_date,
            price_type=accepted_intent.price_type,
            price=Decimal("8.0"),
            status_msg="accepted at submit and filled later",
            order_remark=accepted_intent.order_remark,
        )
    )
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)
    reconciled = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    pending = repo.get_simulation_daily_run(run.run_id)

    assert reconciled.results[0].status == "RECONCILIATION_PENDING_OPEN_ORDERS"
    assert pending.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert pending.run_payload_json["reconcile_after_submit"]["open_order_evidence"]["open_order_count"] == 1

    snapshot_client._orders = [
        {
            "order_id": open_order_id,
            "order_sysid": "sys_900000003",
            "stock_code": accepted_intent.symbol,
            "order_type": accepted_intent.order_type,
            "order_volume": accepted_intent.quantity,
            "price_type": accepted_intent.price_type,
            "price": "8.0",
            "traded_volume": accepted_intent.quantity,
            "traded_price": "8.0",
            "order_status": STATUS_FILLED,
            "status_msg": "filled by broker before close",
            "strategy_name": accepted_intent.strategy_name,
            "order_remark": accepted_intent.order_remark,
        }
    ]
    snapshot_client._trades = [
        {
            "traded_id": "trade_900000003",
            "stock_code": accepted_intent.symbol,
            "order_type": accepted_intent.order_type,
            "traded_time": "14:30:00",
            "traded_price": "8.0",
            "traded_volume": accepted_intent.quantity,
            "traded_amount": str(Decimal("8.0") * Decimal(accepted_intent.quantity)),
            "order_id": open_order_id,
            "order_sysid": "sys_900000003",
            "commission": "0",
            "strategy_name": accepted_intent.strategy_name,
            "order_remark": accepted_intent.order_remark,
        }
    ]
    before_post_close_calls = len(snapshot_client.calls)
    post_close = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 15, 5),
    )
    latest = repo.get_simulation_daily_run(run.run_id)

    assert post_close.stale_terminalized_count == 1
    assert post_close.results[0].status == "POST_CLOSE_TERMINALIZED"
    assert latest.status != SimulationDailyRunStatus.FAILED_TERMINAL
    assert latest.status == SimulationDailyRunStatus.SUCCEEDED
    assert snapshot_client.calls[before_post_close_calls:] == ["orders:False", "trades", "positions"]
    terminalization = latest.run_payload_json["miniqmt_post_close_terminalization"]
    assert terminalization["reason"] == "miniqmt_post_close_batch_succeeded"
    assert terminalization["previous_open_order_evidence"]["open_order_count"] == 1
    assert terminalization["open_order_evidence"]["open_order_count"] == 0
    assert terminalization["fresh_reconcile"]["source"] == "qmt_broker_snapshot_and_strategy_ledger"
    assert terminalization["fresh_reconcile"]["sync_evidence"]["orders_seen"] == 1
    assert terminalization["fresh_reconcile"]["sync_evidence"]["trades_seen"] == 1
    assert terminalization["fresh_reconcile"]["sync_payload_key"] == "sync_after_submit"
    assert terminalization["fresh_reconcile"]["reconcile_payload_key"] == "reconcile_after_submit"


def test_scheduler_post_close_reconcile_failure_is_loud() -> None:
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
            lot_id="lot_scheduler_qmt_post_close_loud",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_post_close_loud",
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
    snapshot_client = FailingQmtSnapshotClient(
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

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    accepted_intent = next(
        intent
        for intent in qmt_repo.list_order_intents_by_batch(run.run_payload_json["qmt_batch_id"])
        if intent.submit_status == IntentSubmitStatus.ACCEPTED
    )
    qmt_repo.upsert_order_ledger(
        OrderLedgerRecord(
            intent_id=accepted_intent.intent_id,
            strategy_id=accepted_intent.strategy_id,
            strategy_name=accepted_intent.strategy_name,
            qmt_order_id="900000004",
            symbol=accepted_intent.symbol,
            order_type=accepted_intent.order_type,
            order_volume=accepted_intent.quantity,
            traded_volume=0,
            order_status=STATUS_OPEN_LIKE,
            account_id=accepted_intent.account_id,
            trade_date=accepted_intent.trade_date,
            price_type=accepted_intent.price_type,
            price=Decimal("8.0"),
            status_msg="accepted but broker query fails at close",
            order_remark=accepted_intent.order_remark,
        )
    )
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)
    reconciled = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    assert reconciled.results[0].status == "RECONCILIATION_PENDING_OPEN_ORDERS"
    snapshot_client.fail = True

    with pytest.raises(DataUnavailableError) as exc_info:
        scheduler.run_once(
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
            submit=True,
            as_of_time=datetime(2026, 5, 21, 15, 5),
            raise_on_error=True,
        )

    assert exc_info.value.context["reason_code"] == "MINIQMT_POST_CLOSE_FRESH_RECONCILE_FAILED"
    assert exc_info.value.context["run_id"] == run.run_id
    assert exc_info.value.context["binding_id"] == qmt_binding.binding_id
    assert exc_info.value.context["error_type"] == "RuntimeError"
    assert exc_info.value.context["error_message"] == "broker snapshot unavailable"
    latest = repo.get_simulation_daily_run(run.run_id)
    assert latest.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert latest.run_payload_json["reconcile_after_submit"]["open_order_evidence"]["open_order_count"] == 1
    assert "miniqmt_post_close_terminalization" not in latest.run_payload_json


def test_scheduler_post_close_terminalizes_dependent_buy_residual_as_retryable_failure() -> None:
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
            cash=Decimal("3500"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_post_close_dependent",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_post_close_dependent",
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

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    assert run.run_payload_json["qmt_batch_result"]["results"][1]["preflight"]["primary_error_code"] == "SELL_PROCEEDS_REQUIRED"
    assert run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)
    post_close = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 15, 5),
    )
    latest = repo.get_simulation_daily_run(run.run_id)

    assert post_close.stale_terminalized_count == 1
    assert latest.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    terminalization = latest.run_payload_json["miniqmt_post_close_terminalization"]
    assert terminalization["audit_state"] == "failed_retryable_after_close"
    assert terminalization["reason"] == "miniqmt_post_close_buy_residual_unresolved"
    assert terminalization["residual_summary"]["dependent_buy_count"] == 1
    assert len(broker.place_order_payloads) == 1


def test_scheduler_rebuilds_side_effect_free_miniqmt_failed_plan_with_fresh_context() -> None:
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
            lot_id="lot_scheduler_qmt_stale_000003_rebuild",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_stale_000003_rebuild",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker(positions=[])
    calendar = StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE])
    manifest = _score_weighted_manifest(release)

    def context_with_positions(positions: dict[str, PositionLot]) -> SimulationRunContext:
        return SimulationRunContext(
            portfolio_id="portfolio_qmt",
            current_positions=positions,
            current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
            manifest=manifest,
            managed_order_service=QmtManagedOrderService(
                repository=qmt_repo,
                broker=broker,  # type: ignore[arg-type]
                calendar_provider=calendar,
            ),
            qmt_ledger_repository=qmt_repo,
            qmt_sync_service=QmtStrategyLedgerSyncService(
                repository=qmt_repo,
                qmt_client=FakeQmtSnapshotClient(positions=[]),
                account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                trade_date=TRADE_DATE,
                calendar_provider=calendar,
            ),
            qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
            broker_positions=[],
            cash=100000.0,
        )

    contexts = [
        context_with_positions(
            {
                "000003.SZ": PositionLot(
                    portfolio_id="portfolio_qmt",
                    symbol="000003.SZ",
                    quantity=77,
                    available_quantity=77,
                    avg_cost=8.0,
                    trade_date=date(2026, 5, 20),
                )
            }
        ),
        context_with_positions({}),
    ]

    class RotatingContextProvider:
        def load_context(self, *, runtime_release, binding, trade_date):
            return contexts.pop(0) if contexts else context_with_positions({})

    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=RotatingContextProvider(),
    )

    failed = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    failed_run = repo.get_simulation_daily_run(failed.results[0].run.run_id)
    failed_plan = repo.get_execution_plan(failed_run.execution_plan_id)

    assert failed.results[0].status == "BROKER_PRECHECK_FAILED"
    assert failed_run.run_payload_json["qmt_batch_status"] == "PREFLIGHT_FAILED"
    assert failed_run.run_payload_json["broker_called"] is False
    assert {intent.symbol for intent in failed_plan.intents if intent.side == OrderSide.SELL} == {"000003.SZ"}

    retried = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    retried_run = repo.get_simulation_daily_run(failed_run.run_id)
    retried_plan = repo.get_execution_plan(retried_run.execution_plan_id)

    assert retried.results[0].execution_plan.plan_id == retried_plan.plan_id
    assert retried_plan.plan_id != failed_plan.plan_id
    assert retried_run.run_payload_json["rebuilt_after_side_effect_free_failure"] is True
    assert retried_run.run_payload_json["rebuilt_from_execution_plan_id"] == failed_plan.plan_id
    assert {intent.symbol for intent in retried_plan.intents if intent.side == OrderSide.SELL} == set()
    assert "BATCH_INSUFFICIENT_BROKER_CAN_SELL" not in str(retried_run.run_payload_json["qmt_batch_result"])
    assert broker.place_order_payloads


def test_scheduler_retries_deferred_miniqmt_dependent_buys_without_duplicate_sells() -> None:
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
            cash=Decimal("3500"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_000003_dependent_buy",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_000003_dependent_buy",
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

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    first_run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    first_batch = qmt_repo.get_order_batch(first_run.run_payload_json["qmt_batch_id"])

    assert first.results[0].status == "BROKER_SUBMIT_FAILED"
    assert first_run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert first_run.run_payload_json["qmt_batch_status"] == OrderBatchStatus.PARTIAL.value
    assert first_run.run_payload_json["broker_called"] is True
    assert first_batch is not None
    assert first_batch.metadata["dependent_buy_deferred"] is True
    assert first_run.run_payload_json["reconcile_after_submit"]["submit_result_gate"]["status"] == "blocked"
    assert [payload["order_type"] for payload in broker.place_order_payloads] == [SELL_ORDER_TYPE]

    account_after_sell = qmt_repo.get_virtual_account(qmt_binding.strategy_id)
    qmt_repo.update_virtual_account(replace(account_after_sell, cash=Decimal("100000")))
    second = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    recovered_run = repo.get_simulation_daily_run(first_run.run_id)
    recovered_batch = qmt_repo.get_order_batch(first_run.run_payload_json["qmt_batch_id"])

    assert second.results[0].status == "RECONCILED"
    assert recovered_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert recovered_run.run_payload_json["qmt_batch_status"] == OrderBatchStatus.SUCCEEDED.value
    assert recovered_run.run_payload_json["qmt_retry_of_batch_id"] == first_run.run_payload_json["qmt_batch_id"]
    assert recovered_batch is not None
    assert recovered_batch.metadata["dependent_buy_deferred"] is False
    assert recovered_batch.metadata["dependent_buy_retry"] is True
    assert [payload["order_type"] for payload in broker.place_order_payloads] == [SELL_ORDER_TYPE, BUY_ORDER_TYPE]


def test_scheduler_polls_succeeded_miniqmt_run_for_late_broker_fill_sync() -> None:
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
    broker = FakeManagedOrderBroker(order_ids=[1082130454])
    broker.positions = []
    snapshot_client = FakeQmtSnapshotClient(orders=[], trades=[], positions=[])
    managed_order_service = QmtManagedOrderService(
        repository=qmt_repo,
        broker=broker,  # type: ignore[arg-type]
        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
    )
    context = SimulationRunContext(
        portfolio_id="portfolio_qmt",
        current_positions={},
        current_prices={"301369.SZ": 180.08},
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
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(
            release,
            candidates=[
                SelectionCandidate(
                    symbol="301369.SZ",
                    score=0.99,
                    rank=1,
                    target_quantity=200,
                    target_weight=0.10,
                    reference_price=180.08,
                    reason="daily_strategy_buy_or_retain",
                )
            ],
        ),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={qmt_binding.binding_id: context}),
    )

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    first_run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    assert first_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert first_run.run_payload_json["broker_called"] is True
    assert first_run.run_payload_json["sync_after_submit"]["trades_seen"] == 0
    assert qmt_repo.list_position_lots(qmt_binding.strategy_id, symbol="301369.SZ") == []
    assert len(broker.place_order_payloads) == 1

    order_remark = broker.place_order_payloads[0]["order_remark"]
    snapshot_client._orders = [
        {
            "order_id": "1082130454",
            "order_sysid": "91",
            "stock_code": "301369.SZ",
            "order_type": 23,
            "order_volume": 200,
            "price_type": 5,
            "price": 180.08,
            "traded_volume": 200,
            "traded_price": 186.2,
            "order_status": 56,
            "strategy_name": qmt_binding.strategy_name,
            "order_remark": order_remark,
        }
    ]
    snapshot_client._trades = [
        {
            "traded_id": "1010000032502320",
            "stock_code": "301369.SZ",
            "order_type": 23,
            "traded_time": "092935",
            "traded_price": 186.2,
            "traded_volume": 200,
            "traded_amount": 37240,
            "order_id": "1082130454",
            "order_sysid": "91",
            "commission": 0,
            "strategy_name": qmt_binding.strategy_name,
            "order_remark": order_remark,
        }
    ]
    broker.positions = [{"stock_code": "301369.SZ", "quantity": 200, "can_sell": 0}]

    reconciled = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    latest_run = repo.get_simulation_daily_run(first_run.run_id)
    lots = qmt_repo.list_position_lots(qmt_binding.strategy_id, symbol="301369.SZ")
    assert reconciled.reused_count == 1
    assert reconciled.results[0].sync_result["trades_inserted"] == 1
    assert reconciled.results[0].reconciliation_result["run"]["status"] == "SUCCEEDED"
    assert latest_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert latest_run.run_payload_json["sync_before_submit"]["trades_inserted"] == 1
    assert latest_run.run_payload_json["reconcile_after_submit"]["run"]["summary_json"]["sync_summary"]["trades_existing"] == 1
    assert [(lot.open_trade_id, lot.remaining_quantity) for lot in lots] == [("1010000032502320", 200)]
    assert len(broker.place_order_payloads) == 1


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


def test_scheduler_recovers_miniqmt_retryable_run_with_order_ledger_evidence_by_reconcile_only() -> None:
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
            lot_id="lot_scheduler_qmt_ledger_side_effect",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_ledger_side_effect",
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
    context = SimulationRunContext(
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
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={qmt_binding.binding_id: context}),
    )

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    failed_run = repo.update_simulation_daily_run(
        submitted.results[0].run.run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        payload_patch={
            "broker_called": False,
            "reconcile_after_submit": {
                "side_effect_evidence": {
                    "schema_version": "miniqmt_broker_side_effect_evidence_v1",
                    "broker_side_effect_count": 1,
                }
            },
        },
    )
    placed_count = len(broker.place_order_payloads)

    recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    recovered_run = repo.get_simulation_daily_run(failed_run.run_id)
    assert recovered.results[0].status == "RECONCILED"
    assert recovered_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert recovered_run.run_payload_json["reconcile_after_submit"]["side_effect_evidence"]["broker_side_effect_count"] > 0
    assert len(broker.place_order_payloads) == placed_count


def test_scheduler_terminalizes_stale_historical_miniqmt_planning_runs_before_today_tick() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions={},
                    current_prices={"000001.SZ": 10.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(
                        repository=InMemoryQmtStrategyLedgerRepository(),
                        broker=FakeManagedOrderBroker(),  # type: ignore[arg-type]
                        calendar_provider=StaticTradingCalendarProvider([TRADE_DATE]),
                    ),
                )
            }
        ),
    )
    stale = scheduler.run_once(
        trade_date=date(2026, 5, 20),
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
        created_by="codex_final_minqmt_multistrategy_dry_run_20260603",
    )
    stale_run = repo.get_simulation_daily_run(stale.results[0].run.run_id)

    today = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    terminalized = repo.get_simulation_daily_run(stale_run.run_id)

    assert today.stale_terminalized_count == 1
    assert today.stale_run_results[0]["run_id"] == stale_run.run_id
    assert terminalized.status == SimulationDailyRunStatus.CANCELLED
    assert terminalized.run_payload_json["stale_active_terminalization"]["previous_status"] == "PLANNING_EXECUTION"
    assert terminalized.run_payload_json["stale_active_terminalization"]["had_broker_side_effect"] is False


def test_scheduler_terminalizes_stale_historical_localsim_planning_runs_before_today_tick() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_local_stale")}
        ),
    )
    stale = scheduler.run_once(
        trade_date=date(2026, 5, 20),
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        created_by="unit_test_localsim_stale",
    )
    stale_run = repo.get_simulation_daily_run(stale.results[0].run.run_id)

    today = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    terminalized = repo.get_simulation_daily_run(stale_run.run_id)
    evidence = terminalized.run_payload_json["localsim_stale_active_terminalization"]

    assert today.stale_terminalized_count == 1
    assert today.stale_run_results[0]["run_id"] == stale_run.run_id
    assert today.stale_run_results[0]["reason_code"] == "LOCALSIM_STALE_ACTIVE_WITHOUT_BROKER_SIDE_EFFECT"
    assert terminalized.status == SimulationDailyRunStatus.CANCELLED
    assert evidence["reason_code"] == "LOCALSIM_STALE_ACTIVE_WITHOUT_BROKER_SIDE_EFFECT"
    assert evidence["previous_status"] == "PLANNING_EXECUTION"
    assert evidence["had_broker_side_effect"] is False


def test_scheduler_post_close_terminalizes_localsim_persisted_active_run_with_shanghai_eod() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    paper_repo = InMemoryPaperTradingV2Repository()
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _local_sim_context_with_real_broker(
                    portfolio_id="portfolio_local_post_close",
                    release=release,
                    paper_repository=paper_repo,
                )
            }
        ),
    )
    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)

    post_close = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 7, 5, tzinfo=UTC),
    )
    latest = repo.get_simulation_daily_run(run.run_id)
    terminalization = latest.run_payload_json["localsim_post_close_terminalization"]

    assert post_close.stale_terminalized_count == 1
    assert post_close.results[0].status == "POST_CLOSE_TERMINALIZED"
    assert post_close.stale_run_results[0]["run_id"] == run.run_id
    assert post_close.stale_run_results[0]["reason_code"] == "LOCALSIM_POST_CLOSE_PERSISTED_SUCCESS"
    assert latest.status == SimulationDailyRunStatus.SUCCEEDED
    assert terminalization["as_of_time"] == "2026-05-21T15:05:00+08:00"
    assert terminalization["reason_code"] == "LOCALSIM_POST_CLOSE_PERSISTED_SUCCESS"
    assert terminalization["local_sim_persistence_status"] == "PERSISTED"


def test_scheduler_miniqmt_account_level_reconciliation_warning_does_not_fail_current_slot() -> None:
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
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="stale_other_slot",
            strategy_name="StaleOtherSlot",
            display_name="Stale Other Slot",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_current_slot_ok",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_current_slot_ok",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_stale_other_slot",
            strategy_id="stale_other_slot",
            symbol="000004.SZ",
            open_trade_id="trade_scheduler_qmt_stale_other_slot",
            open_date=date(2026, 5, 20),
            quantity=500,
            available_quantity=500,
            remaining_quantity=500,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("4000.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
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

    run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    reconciliation = run.run_payload_json["reconcile_after_submit"]
    assert submitted.results[0].status == "RECONCILED"
    assert run.status == SimulationDailyRunStatus.SUCCEEDED
    assert run.run_payload_json["broker_called"] is True
    assert reconciliation["run"]["status"] == "WARNING"
    assert reconciliation["strategy_scope"]["status"] == "SUCCEEDED"
    assert reconciliation["strategy_scope"]["account_level_issue_count"] == 1
    assert reconciliation["run_status_gate"]["status"] == "SUCCEEDED"
    assert reconciliation["run_status_gate"]["reason"] == "strategy_scope_has_no_blocking_issues"


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
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_000003_reconcile_warning",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_000003_reconcile_warning",
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
    reconciliation = submitted.results[0].run.run_payload_json["reconcile_after_submit"]
    assert reconciliation["position_authority"] == "broker_positions"
    assert reconciliation["issues"][0]["issue_type"] == "UNBACKED_STRATEGY_POSITION"
    assert reconciliation["strategy_lot_quantities"]["SchedulerQMT"]["000003.SZ"] == 1
    assert reconciliation["raw_strategy_lot_quantities"]["SchedulerQMT"]["000003.SZ"] == 77


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
        as_of_time=datetime(2026, 5, 21, 1, 22, tzinfo=UTC),
    )

    assert status["restart_recovery_mode"] == "persisted_state_only"
    assert [window["window_id"] for window in status["schedule_windows"]] == [
        "pre_open",
        "selection",
        "planning",
        "execution",
        "post_close_reconcile",
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
    background = SimulationLifecycleBackgroundScheduler(
        lifecycle_scheduler=lifecycle,
        trading_calendar_service=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
    )

    result = background.run_once(as_of_time=datetime(2026, 5, 21, 1, 22, tzinfo=UTC))

    assert result["should_run"] is True
    assert result["submit"] is False
    assert result["window"]["window_id"] == "planning"
    assert result["trading_calendar"]["is_trading_day"] is True
    assert result["summary"]["planned_count"] == 2
    assert background.status()["default_submit"] is False
    assert background.status()["last_result"]["summary"]["total_bindings"] == 2


def test_background_scheduler_skips_non_trading_day_before_lifecycle_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SpyLifecycleScheduler:
        def __init__(self) -> None:
            self.run_once_calls: list[dict[str, Any]] = []
            self.post_close_calls: list[dict[str, Any]] = []

        def status(self) -> dict[str, Any]:
            return {"ok": True, "scheduler": "spy_lifecycle_scheduler"}

        def run_once(self, **kwargs):
            self.run_once_calls.append(kwargs)
            raise AssertionError("non-trading day must not call lifecycle run_once")

        def post_close_reconcile_once(self, **kwargs):
            self.post_close_calls.append(kwargs)
            raise AssertionError("non-trading day must not call post-close reconcile")

    class StatusCalendar:
        def __init__(self) -> None:
            self.calls: list[date | None] = []

        def status(self, *, as_of_date: date | None = None) -> dict[str, Any]:
            self.calls.append(as_of_date)
            return {
                "ok": True,
                "as_of_date": "2026-06-19",
                "is_trading_day": False,
                "next_trading_day": "2026-06-22",
            }

    next_trading_day = date(2026, 6, 22)
    monkeypatch.setenv("SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT", "true")
    lifecycle = SpyLifecycleScheduler()
    calendar = StatusCalendar()
    background = SimulationLifecycleBackgroundScheduler(
        lifecycle_scheduler=lifecycle,  # type: ignore[arg-type]
        trading_calendar_service=calendar,
    )

    result = background.run_once(as_of_time=datetime(2026, 6, 19, 1, 30, tzinfo=UTC))

    assert result["window"]["window_id"] == "execution"
    assert result["should_run"] is False
    assert result["submit"] is False
    assert result["reason"] == "non_trading_day"
    assert result["skip_reason"] == "non_trading_day"
    assert result["next_trading_day"] == next_trading_day.isoformat()
    assert result["trading_calendar"]["is_trading_day"] is False
    assert calendar.calls == [date(2026, 6, 19)]
    assert result["processed"] == []
    assert result["errors"] == []
    assert lifecycle.run_once_calls == []
    assert lifecycle.post_close_calls == []
    assert background.status()["last_result"]["reason"] == "non_trading_day"


def test_lifecycle_scheduler_blocks_non_trading_day_before_roll_forward_or_selection() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None

    class StatusCalendar:
        def __init__(self) -> None:
            self.calls: list[date | None] = []

        def status(self, *, as_of_date: date | None = None) -> dict[str, Any]:
            self.calls.append(as_of_date)
            return {
                "ok": True,
                "as_of_date": "2026-06-20",
                "is_trading_day": False,
                "next_trading_day": "2026-06-22",
            }

    class ExplodingContextProvider:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def load_context(self, **kwargs):
            self.calls.append(kwargs)
            raise AssertionError("non-trading day gate must run before LocalSim context loading")

    context_provider = ExplodingContextProvider()
    calendar = StatusCalendar()
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=context_provider,  # type: ignore[arg-type]
        trading_calendar_service=calendar,
    )

    with pytest.raises(DataUnavailableError) as exc_info:
        scheduler.run_once(
            trade_date=date(2026, 6, 20),
            data_source="DB_HISTORICAL",
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            submit=True,
        )

    assert exc_info.value.context["reason_code"] == "SIMULATION_LIFECYCLE_NON_TRADING_DAY"
    assert exc_info.value.context["next_trading_day"] == "2026-06-22"
    assert calendar.calls == [date(2026, 6, 20)]
    assert context_provider.calls == []
    assert repo.list_simulation_daily_runs(trade_date=date(2026, 6, 20), limit=10) == []


def test_background_scheduler_fails_closed_when_trading_calendar_is_unavailable() -> None:
    class MissingCalendar:
        def is_trading_day(self, trade_date: date) -> bool:
            raise DataUnavailableError(
                "calendar missing",
                context={"trade_date": trade_date.isoformat()},
            )

    class SpyLifecycleScheduler:
        def __init__(self) -> None:
            self.run_once_calls: list[dict[str, Any]] = []

        def run_once(self, **kwargs):
            self.run_once_calls.append(kwargs)
            raise AssertionError("calendar failure must not call lifecycle run_once")

    lifecycle = SpyLifecycleScheduler()
    background = SimulationLifecycleBackgroundScheduler(
        lifecycle_scheduler=lifecycle,  # type: ignore[arg-type]
        trading_calendar_service=MissingCalendar(),
    )

    result = background.run_once(as_of_time=datetime(2026, 6, 19, 1, 30, tzinfo=UTC))

    assert result["should_run"] is False
    assert result["submit"] is False
    assert result["reason"] == "trading_calendar_unavailable"
    assert result["processed"] == []
    assert result["errors"][0]["type"] == "DataUnavailableError"
    assert result["errors"][0]["context"] == {"trade_date": "2026-06-19"}
    assert lifecycle.run_once_calls == []


def test_background_scheduler_runs_post_close_reconcile_without_submit_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
            cash=Decimal("1"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_background_post_close",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_background_post_close",
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
    context = SimulationRunContext(
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
            qmt_client=FakeQmtSnapshotClient(
                positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}]
            ),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            trade_date=TRADE_DATE,
            calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
        ),
        qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
        broker_positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
    )
    lifecycle = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={qmt_binding.binding_id: context}),
    )
    submitted = lifecycle.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)
    monkeypatch.setenv("SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT", "false")
    background = SimulationLifecycleBackgroundScheduler(
        lifecycle_scheduler=lifecycle,
        trading_calendar_service=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
    )

    result = background.run_once(as_of_time=datetime(2026, 5, 21, 7, 5, tzinfo=UTC))
    latest = repo.get_simulation_daily_run(run.run_id)

    assert result["should_run"] is True
    assert result["submit"] is False
    assert result["window"]["window_id"] == "post_close_reconcile"
    assert result["trading_calendar"]["is_trading_day"] is True
    assert result["processed"] == []
    assert result["summary"]["stale_terminalized_count"] == 1
    assert result["terminalized_runs"][0]["run_id"] == run.run_id
    assert latest.status == SimulationDailyRunStatus.SUCCEEDED
    assert len(broker.place_order_payloads) == 1


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


def test_scheduler_marks_pre_trade_blocked_holding_without_broker_submit() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    blocked_position = {
        "688689.SH": PositionLot(
            portfolio_id="portfolio_blocked",
            symbol="688689.SH",
            quantity=878,
            available_quantity=878,
            avg_cost=46.82,
            trade_date=TRADE_DATE - timedelta(days=1),
        )
    }
    context = SimulationRunContext(
        portfolio_id="portfolio_blocked",
        current_positions=blocked_position,
        current_prices={"688689.SH": 46.82},
        local_broker=FakeLocalSimBroker(),
        pre_trade_tradability={
            "688689.SH": {
                "schema_version": "pre_trade_tradability_status_v1",
                "symbol": "688689.SH",
                "trade_date": TRADE_DATE.isoformat(),
                "is_tradable": False,
                "reason_code": "NO_TRADABLE_REALTIME_QUOTE",
                "source": "TDX_REALTIME.batch_quote",
                "quote_evidence": {
                    "open": 0,
                    "high": 0,
                    "low": 0,
                    "total_hand": 0,
                    "bid_price_1": 0,
                    "ask_price_1": 0,
                    "no_tradable_market": True,
                },
            }
        },
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=[], valid_no_candidate=True),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: context}),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    latest_run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    assert result.results[0].status == "PRE_TRADE_BLOCKED"
    assert latest_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert latest_run.run_payload_json["broker_called"] is False
    assert latest_run.run_payload_json["no_rebalance_required"] is False
    assert latest_run.run_payload_json["pre_trade_blocked_order_generation"]["blocked_symbols"] == ["688689.SH"]
    assert result.results[0].execution_plan.intents == []
    assert result.results[0].execution_plan.trading_rule_decisions[0].reason_code == "NO_TRADABLE_REALTIME_QUOTE"
    assert context.local_broker.submitted == []


def test_scheduler_marks_existing_zero_intent_plan_success_when_submit_window_reuses_it() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    fake_selection = FakeSelectionService(release, candidates=[], valid_no_candidate=True)
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=fake_selection,
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_empty_qmt",
                    current_positions={},
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
    resumed = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    assert planned.planned_count == 1
    assert planned.results[0].execution_plan.intents == []
    assert resumed.submitted_count == 1
    assert resumed.reused_count == 0
    assert resumed.results[0].status == "NO_REBALANCE"
    assert len(fake_selection.calls) == 1
    latest = repo.get_simulation_daily_run(planned.results[0].run.run_id)
    assert latest.status == SimulationDailyRunStatus.SUCCEEDED
    assert latest.run_payload_json["no_rebalance_required"] is True
    assert latest.run_payload_json["broker_called"] is False
    assert latest.run_payload_json["last_stage"] == "SUCCEEDED"


def test_scheduler_runs_two_localsim_strategies_with_independent_state_and_restart_idempotency() -> None:
    release, local_binding_a, _, repo = _release_and_bindings()
    assert local_binding_a is not None
    local_binding_b = _create_extra_binding(
        release=release,
        repo=repo,
        strategy_id="strategy_local_scheduler_b",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
    )
    paper_repo = InMemoryPaperTradingV2Repository()
    selection = FakeSelectionService(release, candidates=_candidate_rows())
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=selection,
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding_a.binding_id: _local_sim_context_with_real_broker(
                    portfolio_id="portfolio_local_a",
                    release=release,
                    paper_repository=paper_repo,
                ),
                local_binding_b.binding_id: _local_sim_context_with_real_broker(
                    portfolio_id="portfolio_local_b",
                    release=release,
                    paper_repository=paper_repo,
                    positions={
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
    for item in submitted_by_strategy.values():
        run_id = item.run.run_id
        assert paper_repo.list_orders_for_run(run_id)
        assert paper_repo.list_fills_for_run(run_id)
        assert paper_repo.cash_entries[run_id]
    assert len(selection.calls) == 2
    assert restarted.reused_count == 2


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
        execution_policy={
            "validated_execution_policy_id": "exec_policy_close_price",
            "policy_sha256": "policy_sha256",
            "policy_json": {
                "algo_code": "CLOSE_PRICE",
                "algo_config": {"allow_partial_fill": True},
            },
        },
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
        execution_policy={
            "validated_execution_policy_id": "exec_policy_close_price",
            "policy_sha256": "policy_sha256",
            "policy_json": {
                "algo_code": "CLOSE_PRICE",
                "algo_config": {"allow_partial_fill": True},
            },
        },
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
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        position_loader=lambda strategy_id, trade_date: positions,
        price_loader=lambda symbols, trade_date: {symbol: 12.6 for symbol in symbols},
        managed_order_service_factory=lambda: "fake_mos",
        qmt_sync_service_factory=lambda: "fake_sync",
        qmt_reconciliation_service_factory=lambda: "fake_recon",
        qmt_ledger_repository=qmt_repo,
        package_manifest_loader=lambda package_id: manifest,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)
    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())
    assert ctx.current_positions == positions
    assert ctx.current_prices == {"000001.XSHE": 12.6}
    assert ctx.manifest == manifest
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
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 10.5 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: qmt_client,
        package_manifest_loader=lambda package_id: manifest,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    assert ctx.current_positions["000001.SZ"].quantity == 1000
    assert ctx.current_prices == {"000001.SZ": 10.5}
    assert ctx.manifest == manifest
    assert ctx.cash == 900000
    assert ctx.frozen_cash == 123
    assert ctx.realized_pnl == 45
    assert ctx.qmt_ledger_repository is qmt_repo
    assert ctx.qmt_sync_service is not None
    assert ctx.qmt_reconciliation_service is not None
    assert getattr(ctx.managed_order_service, "_broker") is qmt_client


def test_production_context_provider_applies_miniqmt_pre_trade_tradability_gate_today() -> None:
    """MiniQMT production context loads suspend/no-quote evidence before plan generation."""
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
            lot_id="lot_suspended_no_quote",
            strategy_id="strat1",
            symbol="688689.SH",
            open_trade_id="trade_suspended_no_quote",
            open_date=date.today() - timedelta(days=1),
            quantity=878,
            available_quantity=878,
            remaining_quantity=878,
            avg_cost=Decimal("46.82"),
            cost_amount=Decimal("41111.96"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    tradability = FakePreTradeTradabilityProvider(
        {
            "688689.SH": {
                "schema_version": "pre_trade_tradability_status_v1",
                "symbol": "688689.SH",
                "trade_date": date.today().isoformat(),
                "is_tradable": False,
                "reason_code": "NO_TRADABLE_REALTIME_QUOTE",
                "source": "TDX_REALTIME.batch_quote",
                "quote_evidence": {
                    "open": 0,
                    "high": 0,
                    "low": 0,
                    "total_hand": 0,
                    "bid_price_1": 0,
                    "ask_price_1": 0,
                    "no_tradable_market": True,
                },
            }
        }
    )
    broker = FakeManagedOrderBroker(positions=[{"stock_code": "688689.SH", "quantity": 878, "can_sell": 878}])
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 46.82 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        pre_trade_tradability_provider=tradability,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())

    assert ctx.pre_trade_tradability["688689.SH"]["reason_code"] == "NO_TRADABLE_REALTIME_QUOTE"
    assert ctx.context_diagnostics["pre_trade_tradability"]["blocked_symbols"] == [
        {"symbol": "688689.SH", "reason_code": "NO_TRADABLE_REALTIME_QUOTE", "source": "TDX_REALTIME.batch_quote"}
    ]
    assert tradability.calls[-1]["require_realtime_quote"] is True


def test_production_context_provider_drops_miniqmt_stale_lots_missing_from_broker():
    """MiniQMT current_positions must not emit impossible sells for lots absent from broker can_sell."""
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
            lot_id="lot_stale_not_in_broker",
            strategy_id="strat1",
            symbol="000636.SZ",
            open_trade_id="trade_stale_not_in_broker",
            open_date=date(2026, 5, 20),
            quantity=900,
            available_quantity=900,
            remaining_quantity=900,
            avg_cost=Decimal("10.00"),
            cost_amount=Decimal("9000"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    seen_price_symbols: list[tuple[str, ...]] = []

    def price_loader(symbols, trade_date):
        seen_price_symbols.append(tuple(symbols))
        return {symbol: 10.5 for symbol in symbols}

    broker = FakeManagedOrderBroker(positions=[])
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=price_loader,
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    assert ctx.current_positions == {}
    assert ctx.broker_positions == []
    assert seen_price_symbols == [()]
    diagnostics = ctx.context_diagnostics["miniqmt_broker_position_reconciliation"]
    assert diagnostics["dropped_position_count"] == 1
    assert diagnostics["dropped_positions"][0]["symbol"] == "000636.SZ"


def test_production_context_provider_caps_miniqmt_lots_to_broker_quantity_and_can_sell():
    """Strategy-lot context is capped by broker-authoritative quantity/can_sell before planning."""
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
            lot_id="lot_cap_to_broker",
            strategy_id="strat1",
            symbol="000001.SZ",
            open_trade_id="trade_cap_to_broker",
            open_date=date(2026, 5, 20),
            quantity=1000,
            available_quantity=1000,
            remaining_quantity=1000,
            avg_cost=Decimal("10.00"),
            cost_amount=Decimal("10000"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker(positions=[{"stock_code": "000001.SZ", "quantity": 500, "can_sell": 300}])
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 10.5 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    position = ctx.current_positions["000001.SZ"]
    assert position.quantity == 500
    assert position.available_quantity == 300
    assert ctx.broker_positions == [{"stock_code": "000001.SZ", "quantity": 500, "can_sell": 300}]
    diagnostics = ctx.context_diagnostics["miniqmt_broker_position_reconciliation"]
    assert diagnostics["capped_position_count"] == 1
    assert diagnostics["capped_positions"][0]["reconciled_quantity"] == 500
    assert diagnostics["capped_positions"][0]["reconciled_available_quantity"] == 300


def test_production_context_provider_projects_miniqmt_strategy_slot_from_account_broker_authority():
    """One slot's local lots cannot consume another slot's broker-backed attribution."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    for strategy_id, strategy_name, quantity in (
        ("strat1", "StrategyA", 7600),
        ("strat_b", "StrategyB", 6600),
    ):
        qmt_repo.create_virtual_account(
            VirtualAccount(
                strategy_id=strategy_id,
                strategy_name=strategy_name,
                display_name=strategy_name,
                account_id="QMT_SIM_ACCOUNT",
                mode="SIM",
                initial_cash=Decimal("1000000"),
                cash=Decimal("900000"),
                status=VirtualAccountStatus.ENABLED,
            )
        )
        qmt_repo.create_position_lot(
            PositionLotRecord(
                lot_id=f"lot_{strategy_id}",
                strategy_id=strategy_id,
                symbol="001358.SZ",
                open_trade_id=f"trade_{strategy_id}",
                open_date=date(2026, 5, 20),
                quantity=quantity,
                available_quantity=quantity,
                remaining_quantity=quantity,
                avg_cost=Decimal("29.88"),
                cost_amount=Decimal(quantity) * Decimal("29.88"),
                account_id="QMT_SIM_ACCOUNT",
            )
        )
    broker = FakeManagedOrderBroker(positions=[{"stock_code": "001358.SZ", "quantity": 13200, "can_sell": 13200}])
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 30.0 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    assert ctx.current_positions["001358.SZ"].quantity == 7100
    diagnostics = ctx.context_diagnostics["miniqmt_broker_position_reconciliation"]
    assert diagnostics["position_authority"] == "broker_positions"
    assert diagnostics["account_strategy_count"] == 2
    assert diagnostics["capped_position_count"] == 1
    assert diagnostics["projection_adjustments"][0]["issue_type"] == "UNBACKED_STRATEGY_POSITION"
    assert diagnostics["projection_adjustments"][0]["projected_strategy_quantities"] == {
        "strat1": 7100,
        "strat_b": 6100,
    }


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
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 10.5 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        qmt_calendar_provider_factory=lambda: StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
        package_manifest_loader=lambda package_id: manifest,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)
    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)
    assert ctx.manifest == manifest

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
    manifest = _score_weighted_manifest(release)
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
        package_manifest_loader=lambda package_id: manifest,
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
    quantity_by_symbol = {intent.symbol: intent.quantity for intent in preview_intents}
    assert quantity_by_symbol["000001.SZ"] == 4700
    assert quantity_by_symbol["688001.SH"] == 2389
    assert payload["target_equity_basis"]["source"] == "miniqmt_strategy_slot_dynamic_equity"
    assert payload["target_equity_basis"]["cash"] == 100_000.0
    assert payload["target_equity_basis"]["market_value"] == 616.0
    assert payload["target_equity_basis"]["total_equity"] == 100_616.0
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
        execution_policy={
            "validated_execution_policy_id": "exec_policy_close_price",
            "policy_sha256": "policy_sha256",
            "policy_json": {
                "algo_code": "CLOSE_PRICE",
                "algo_config": {"allow_partial_fill": True},
            },
        },
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
        pre_trade_tradability_provider=FakePreTradeTradabilityProvider(),
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    assert ctx.local_broker is not None
    assert ctx.local_broker.data_source == MinuteDataSource.DB_HISTORICAL
    assert ctx.market_data_source == MinuteDataSource.DB_HISTORICAL.value
    assert ctx.local_broker.query_account().cash == Decimal("980000")
    assert ctx.local_broker.query_positions()["000001.SZ"].quantity == 1000
    assert ctx.local_broker.query_positions()["000001.SZ"].available_quantity == 1000
    assert ctx.context_diagnostics["localsim_tplus1_settlement"]["settled_position_count"] == 0


def test_production_context_provider_settles_localsim_tplus1_positions_for_trade_date():
    """LocalSim unattended context must unlock prior-day Paper v2 lots before rebalance planning."""
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
        execution_policy={
            "validated_execution_policy_id": "exec_policy_close_price",
            "policy_sha256": "policy_sha256",
            "policy_json": {
                "algo_code": "CLOSE_PRICE",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )
    positions = {
        "000001.SZ": PositionLot(
            portfolio_id="strat1",
            symbol="000001.SZ",
            quantity=1000,
            available_quantity=0,
            avg_cost=10.0,
            trade_date=TRADE_DATE - timedelta(days=1),
        )
    }
    paper_repo = FakePaperRepository(portfolio, positions=positions, cash=980_000)
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: paper_repo,
        price_loader=lambda symbols, trade_date: {symbol: 10.5 for symbol in symbols},
        pre_trade_tradability_provider=FakePreTradeTradabilityProvider(),
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    assert ctx.current_positions["000001.SZ"].available_quantity == 1000
    assert ctx.local_broker.query_positions()["000001.SZ"].available_quantity == 1000
    settlement = ctx.context_diagnostics["localsim_tplus1_settlement"]
    assert settlement["settled_position_count"] == 1
    assert settlement["settled_positions"][0]["previous_available_quantity"] == 0


def test_production_context_provider_uses_tdx_realtime_for_same_day_localsim() -> None:
    """Same-day unattended LocalSim must not depend on post-market DB minute sync."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    trade_date = date.today()
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    portfolio = PaperPortfolio(
        portfolio_id="strat1",
        portfolio_name="LocalSim same-day prod context",
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=trade_date,
        data_source=MinuteDataSource.DB_HISTORICAL,
        execution_policy={
            "validated_execution_policy_id": "exec_policy_close_price",
            "policy_sha256": "policy_sha256",
            "policy_json": {
                "algo_code": "CLOSE_PRICE",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )
    positions = {
        "000001.SZ": PositionLot(
            portfolio_id="strat1",
            symbol="000001.SZ",
            quantity=1000,
            available_quantity=1000,
            avg_cost=10.0,
            trade_date=trade_date,
        )
    }
    paper_repo = FakePaperRepository(portfolio, positions=positions, cash=980_000)
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: paper_repo,
        price_loader=lambda symbols, trade_date: {symbol: 10.5 for symbol in symbols},
        pre_trade_tradability_provider=FakePreTradeTradabilityProvider(),
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=trade_date)

    assert ctx.local_broker is not None
    assert ctx.local_broker.data_source == MinuteDataSource.TDX_REALTIME
    assert ctx.market_data_source == MinuteDataSource.TDX_REALTIME.value


def test_production_context_provider_rejects_stale_portfolio_policy_when_release_policy_is_vnpy_id_only() -> None:
    """LocalSim must not fall back to stale portfolio V25 when the runtime release points to vn.py."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    policy_id = "vnpy_asset:SNIPER_MINIQMT:final_multistrategy_dry_run_20260603"
    release = _make_test_release(
        execution_policy_version_id=policy_id,
        execution_policy_sha256="sha_vnpy_release",
        execution_policy={"policy_version_id": policy_id, "policy_sha256": "sha_vnpy_release"},
    )
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    portfolio = PaperPortfolio(
        portfolio_id="strat1",
        portfolio_name="LocalSim stale V25 guard",
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.DB_HISTORICAL,
        execution_policy={
            "validated_execution_policy_id": "exec_policy_v25_1_small_cap",
            "policy_sha256": "sha_portfolio_v25",
            "policy_json": {
                "algo_code": "V25_1_SMALL_CAP",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: FakePaperRepository(portfolio, positions={}, cash=1_000_000),
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)

    with pytest.raises(RuntimeConfigInvalidError, match="snapshot is missing full policy_json") as exc_info:
        provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    assert exc_info.value.context["release_execution_policy_version_id"] == policy_id
    assert exc_info.value.context["portfolio_policy_algo_code"] == "V25_1_SMALL_CAP"
    assert "LocalSim-compatible execution policy" in exc_info.value.context["required_action"]


def test_production_context_provider_uses_runtime_release_policy_snapshot_over_portfolio_default() -> None:
    """Runtime release policy_json is authoritative when present."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    release = _make_test_release(
        execution_policy_version_id="exec_policy_runtime_close",
        execution_policy_sha256="sha_runtime_close",
        execution_policy={
            "policy_version_id": "exec_policy_runtime_close",
            "policy_sha256": "sha_runtime_close",
            "policy_json": {
                "algo_code": "CLOSE_PRICE",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    portfolio = PaperPortfolio(
        portfolio_id="strat1",
        portfolio_name="LocalSim release policy authority",
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.DB_HISTORICAL,
        execution_policy={
            "validated_execution_policy_id": "exec_policy_v25_1_small_cap",
            "policy_sha256": "sha_portfolio_v25",
            "policy_json": {
                "algo_code": "V25_1_SMALL_CAP",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )
    market_data = FakeLocalSimMarketDataProvider()
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: FakePaperRepository(portfolio, positions={}, cash=1_000_000),
        price_loader=lambda symbols, trade_date: {symbol: 10.0 for symbol in symbols},
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)
    assert ctx.execution_policy_payload == release.release_config_json["execution_policy"]
    assert ctx.local_broker is not None
    ctx.local_broker._market_data_provider = market_data
    handle = ctx.local_broker.submit_order_intent(
        OrderIntent(
            package_id=release.package_id,
            portfolio_id="strat1",
            symbol="000001.SZ",
            side=OrderSide.BUY,
            quantity=100,
            order_type=OrderType.MARKET,
            target_trade_date=TRADE_DATE,
        )
    )

    assert ctx.local_broker.query_status(handle).state == "filled"
    assert market_data.calls[-1]["require_day_features"] is False


def test_production_context_provider_uses_portfolio_execution_policy_for_alpha_core_localsim_recovery():
    """Alpha-core LocalSim recovery must use the Paper v2 validated policy snapshot, not manifest.minute_execution_policy."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    portfolio = PaperPortfolio(
        portfolio_id="strat1",
        portfolio_name="LocalSim alpha-core recovery",
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.DB_HISTORICAL,
        execution_policy={
            "validated_execution_policy_id": "exec_policy_close_price",
            "policy_sha256": "policy_sha256",
            "policy_json": {
                "algo_code": "CLOSE_PRICE",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )
    paper_repo = FakePaperRepository(portfolio, positions={}, cash=1_000_000)
    market_data = FakeLocalSimMarketDataProvider()
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: paper_repo,
        price_loader=lambda symbols, trade_date: {symbol: 10.0 for symbol in symbols},
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)
    assert ctx.local_broker is not None
    ctx.local_broker._market_data_provider = market_data
    handle = ctx.local_broker.submit_order_intent(
        OrderIntent(
            package_id=release.package_id,
            portfolio_id="strat1",
            symbol="000001.SZ",
            side=OrderSide.BUY,
            quantity=100,
            order_type=OrderType.MARKET,
            target_trade_date=TRADE_DATE,
        )
    )

    assert ctx.local_broker.query_status(handle).state == "filled"
    assert market_data.calls[-1]["require_day_features"] is False


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


def test_scheduler_rejects_stale_pit_cutoff_selection_evidence_for_trade_date():
    stale_runtime_config = {
        "selection_artifact_config": {
            "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE",
            "cutoff_date": "2026-05-19",
        },
        "point_in_time_context": {
            "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE",
            "trade_date": TRADE_DATE.isoformat(),
            "requested_trade_date": TRADE_DATE.isoformat(),
            "effective_trade_date": TRADE_DATE.isoformat(),
            "cutoff_date": "2026-05-19",
            "score_trade_date": "2026-05-19",
            "reference_price_trade_date": "2026-05-19",
        },
    }
    release, local_binding, _, repo = _release_and_bindings(
        qmt_only=False,
        release_metadata={"selection_runtime_config": stale_runtime_config},
    )
    stale_evidence = _evidence(
        release,
        candidates=_candidate_rows(),
        cutoff_date=date(2026, 5, 19),
    )
    stale_selection = StrategyPackageSelectionResult(
        runtime_config=stale_runtime_config,
        package_results={release.package_id: _candidate_rows()},
        aggregate_results=_candidate_rows(),
        excluded_results={release.package_id: []},
        manifest_sha256_by_package={release.package_id: release.manifest_sha256},
        evidence_by_package={release.package_id: stale_evidence},
    )

    class RollingCalendar:
        def ensure_trading_day(self, trade_date: date) -> None:
            if trade_date != TRADE_DATE:
                raise DataUnavailableError("not a trading day", context={"trade_date": trade_date.isoformat()})

        def list_trading_days(self, start_date: date, end_date: date) -> list[date]:
            return [
                item
                for item in (date(2026, 5, 19), date(2026, 5, 20), TRADE_DATE)
                if start_date <= item <= end_date
            ]

    class StaleCutoffSelectionService:
        def __init__(self) -> None:
            self.resolver = StrategyPackageSelectionService(calendar_provider=RollingCalendar())

        def run_selection(self, **kwargs):
            return stale_selection

        def resolve_point_in_time_context(self, **kwargs):
            return self.resolver.resolve_point_in_time_context(**kwargs)

    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=StaleCutoffSelectionService(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id=local_binding.strategy_id)}
        ),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
    )

    context = result.results[0].error["context"]
    assert result.failed_count == 1
    assert result.results[0].status == "FAILED"
    assert result.results[0].error["type"] == "DataUnavailableError"
    assert "cutoff_date" in context["reasons"]
    assert context["cutoff_date"] == "2026-05-19"
    assert context["expected_cutoff_date"] == "2026-05-20"


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


def _make_test_release(
    *,
    execution_policy_version_id: str = "exec_policy_close_price",
    execution_policy_sha256: str = "policy_sha256",
    execution_policy: dict[str, Any] | None = None,
):
    from backend.services.simulation_runtime.models import StrategyRuntimeRelease
    policy_payload = execution_policy or {
        "policy_version_id": execution_policy_version_id,
        "policy_sha256": execution_policy_sha256,
    }
    return StrategyRuntimeRelease(
        package_id="pkg", manifest_sha256="aa",
        runtime_profile_id="rp", runtime_profile_version_id="rpv", runtime_profile_sha256="rps",
        daily_strategy_profile_version_id="dsp", execution_policy_version_id=execution_policy_version_id,
        execution_policy_sha256=execution_policy_sha256, tail_policy_version_id="tpv", tail_policy_sha256="tps",
        release_config_json={
            "schema_version": "strategy_runtime_release_v1",
            "package_id": "pkg",
            "manifest_sha256": "aa",
            "runtime_profile": {"profile_id": "rp", "profile_version_id": "rpv", "config_sha256": "rps"},
            "daily_strategy": {"profile_version_id": "dsp"},
            "execution_policy": policy_payload,
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
