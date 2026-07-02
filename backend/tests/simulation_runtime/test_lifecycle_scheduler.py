from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest
import backend.services.simulation_runtime.bridges as simulation_bridges

from backend.services.paper_trading_v2.models import PaperPortfolio
from backend.services.paper_trading_v2.market_data import DailyStStatus, MinuteDataSource, MinuteExecutionMarketInput
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
    SimulationDailyRun,
    SimulationLifecycleScheduler,
    SimulationRunContext,
    SimulationRuntimeOpsService,
    StaticSimulationRunContextProvider,
    SimulationSchedulerBindingResult,
    SimulationSchedulerRunOnceResult,
    StrategyPackageSelectionResult,
    StrategyPackageSelectionService,
    StrategyRuntimeReleaseService,
)
from backend.services.simulation_runtime.lifecycle import (
    MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW,
    compute_schedule_windows,
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
from backend.services.strategy_package.live_inference import (
    PREFLIGHT_CHECK_MODEL_PARAMS,
    PREFLIGHT_CHECK_NAMES,
    PREFLIGHT_STATUS_BLOCKED,
    PREFLIGHT_STATUS_PASS,
    LiveInferencePreflightCheck,
    LiveInferencePreflightError,
    LiveInferencePreflightResult,
)
from backend.services.trading_core.errors import BrokerRejectedError, DataUnavailableError, RuntimeConfigInvalidError
from backend.services.trading_core.models import MinuteBar, OrderIntent, OrderSide, OrderType, PositionLot
from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    InMemoryMiniQMTExecutionRuntimeRepository,
    JsonFileMiniQMTExecutionRuntimeRepository,
    MiniQMTExecutionRuntime,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntimeKind,
    MiniQMTGraySwitchController,
    MiniQMTOperatorCommandStatus,
    MiniQMTShadowReconciler,
    MiniQMTShadowScenario,
)
from backend.services.miniqmt_execution_runtime.gray import _shadow_evidence_for_scope


TRADE_DATE = date(2026, 5, 21)


@pytest.fixture(autouse=True)
def _deterministic_scheduler_now(monkeypatch: pytest.MonkeyPatch) -> None:
    import backend.services.simulation_runtime.lifecycle as lifecycle_module
    import backend.services.simulation_runtime.scheduler as scheduler_module

    def fixed_now() -> datetime:
        return datetime(2026, 5, 21, 10, 0, tzinfo=lifecycle_module.SCHEDULER_TZ)

    monkeypatch.setattr(lifecycle_module, "scheduler_now", fixed_now)
    monkeypatch.setattr(scheduler_module, "scheduler_now", fixed_now)


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


def _create_scheduler_release(
    repo: InMemorySimulationRuntimeRepository,
    *,
    package_id: str,
    manifest_sha256: str,
):
    return StrategyRuntimeReleaseService(repository=repo).create_release(
        package_id=package_id,
        manifest_sha256=manifest_sha256,
        runtime_profile_id=f"runtime_profile_{package_id}",
        runtime_profile_version_id=f"runtime_profile_{package_id}_v1",
        runtime_profile_sha256=f"runtime_profile_{package_id}_hash",
        daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        execution_policy_version_id="exec_policy_v25_1_small_cap",
        execution_policy_sha256=f"exec_policy_{package_id}_hash",
        tail_policy_version_id="tail_policy_close_v1",
        tail_policy_sha256=f"tail_policy_{package_id}_hash",
        created_by="unit-test",
        created_reason="scheduler multi-release test",
    )


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


class PackageRoutingSelectionService:
    def __init__(
        self,
        releases_by_package: dict[str, Any],
        *,
        failing_package_id: str,
        exc: Exception | None = None,
    ) -> None:
        self.releases_by_package = dict(releases_by_package)
        self.failing_package_id = failing_package_id
        self.exc = exc or _live_inference_preflight_error(package_id=failing_package_id)
        self.calls: list[dict[str, Any]] = []

    def run_selection(self, **kwargs):
        self.calls.append(kwargs)
        package_id = kwargs["package_ids"][0]
        if package_id == self.failing_package_id:
            raise self.exc
        release = self.releases_by_package[package_id]
        candidates = _candidate_rows()
        evidence = _evidence(release, candidates=candidates, target_trade_date=kwargs.get("trade_date") or TRADE_DATE)
        return StrategyPackageSelectionResult(
            runtime_config={
                "runtime_profile": {
                    "selection": {"daily_strategy_id": DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID}
                }
            },
            package_results={release.package_id: candidates},
            aggregate_results=candidates,
            excluded_results={release.package_id: []},
            manifest_sha256_by_package={release.package_id: release.manifest_sha256},
            evidence_by_package={release.package_id: evidence},
        )


def _live_inference_preflight_error(*, package_id: str) -> LiveInferencePreflightError:
    checks = [
        LiveInferencePreflightCheck(
            name=name,
            status=PREFLIGHT_STATUS_BLOCKED if name == PREFLIGHT_CHECK_MODEL_PARAMS else PREFLIGHT_STATUS_PASS,
            message=(
                "StrategyPackage model params.pkl references local model code that is missing"
                if name == PREFLIGHT_CHECK_MODEL_PARAMS
                else f"{name} passed"
            ),
            context={
                "reason_code": "strategy_package_model_code_missing",
                "package_id": package_id,
                "missing_relative_paths": ["model.py"],
            }
            if name == PREFLIGHT_CHECK_MODEL_PARAMS
            else {},
        )
        for name in PREFLIGHT_CHECK_NAMES
    ]
    preflight = LiveInferencePreflightResult(passed=False, checks=checks)
    return LiveInferencePreflightError(
        "live inference cold-start preflight failed: StrategyPackage model code missing",
        context={
            "source_type": "live_qe_model_inference_v1",
            "source_id": "exp_bad",
            "package_id": package_id,
            "phase": "preflight",
            "blocked_check": PREFLIGHT_CHECK_MODEL_PARAMS,
            "preflight": preflight.to_dict(),
        },
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


class SelectiveFailingContextProvider(StaticSimulationRunContextProvider):
    def __init__(self, *, failing_binding_id: str, exc_factory, **kwargs) -> None:
        super().__init__(**kwargs)
        self.failing_binding_id = failing_binding_id
        self.exc_factory = exc_factory
        self.calls: list[str] = []

    def load_context(self, *, runtime_release, binding, trade_date):
        self.calls.append(binding.binding_id)
        if binding.binding_id == self.failing_binding_id:
            raise self.exc_factory(binding, trade_date)
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


class BoardLotRejectingLocalSimBroker(FakeLocalSimBroker):
    def submit_order_intent(self, intent):
        self.submitted.append(intent)
        raise BrokerRejectedError(
            "LocalSim ledger rejected the order",
            context={
                "intent_id": intent.intent_id,
                "symbol": intent.symbol,
                "quantity": intent.quantity,
                "cause": "fill quantity violates LocalSim board-lot rules; reason_code=LOCAL_SIM_BOARD_LOT_VIOLATION",
                "cause_code": "RISK_RULE_ERROR",
            },
        )


class FakeManagedOrderBroker:
    def __init__(
        self,
        order_ids: list[int] | None = None,
        positions: list[dict[str, Any]] | None = None,
        quotes: dict[str, dict[str, Any]] | None = None,
        connected: bool = True,
        connect_ok: bool = True,
        fail_next_place: bool = False,
        fail_order_query: bool = False,
        fail_trade_query: bool = False,
    ) -> None:
        self.order_ids = list(order_ids or [])
        self.positions = (
            list(positions)
            if positions is not None
            else [{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}]
        )
        self.quotes = dict(quotes or {})
        self.connected = connected
        self.connect_ok = connect_ok
        self.fail_next_place = fail_next_place
        self.fail_order_query = fail_order_query
        self.fail_trade_query = fail_trade_query
        self.place_order_payloads = []
        self.full_tick_calls: list[list[str]] = []
        self.order_query_calls = 0
        self.trade_query_calls = 0

    def status(self):
        return {"connected": self.connected, "mode": "SIM", "provider": "fake"}

    def connect(self):
        self.connected = bool(self.connect_ok)
        return self.connected, "connected" if self.connected else "connect failed"

    def get_positions(self):
        return list(self.positions)

    def get_orders(self, cancelable_only: bool = False):
        self.order_query_calls += 1
        if self.fail_order_query:
            raise RuntimeError("simulated broker order snapshot unavailable")
        return []

    def get_trades(self):
        self.trade_query_calls += 1
        if self.fail_trade_query:
            raise RuntimeError("simulated broker trade snapshot unavailable")
        return []

    def get_full_tick(self, symbols):
        self.full_tick_calls.append(list(symbols))
        return {symbol: dict(self.quotes[symbol]) for symbol in symbols if symbol in self.quotes}

    def place_order(self, **kwargs):
        self.place_order_payloads.append(kwargs)
        if self.fail_next_place:
            self.fail_next_place = False
            self.connected = False
            raise RuntimeError("simulated miniQMT disconnect during submit")
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


def _miniqmt_shadow_test_scheduler(*, candidates: list[SelectionCandidate] | None = None):
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
            lot_id="lot_scheduler_qmt_shadow_000003",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_shadow_000003",
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
        selection_service=FakeSelectionService(release, candidates=candidates or _candidate_rows()),
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
    return scheduler, repo, broker, qmt_binding


def _miniqmt_scheduler_with_ledger_context(
    *,
    cash: Decimal = Decimal("100000"),
    snapshot_client: FakeQmtSnapshotClient | None = None,
):
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
            cash=cash,
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_cross_day_000003",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_cross_day_000003",
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
    snapshot_client = snapshot_client or FakeQmtSnapshotClient(
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
    return scheduler, repo, broker, qmt_binding, qmt_repo, snapshot_client


def _runtime_store_shadow_events(path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [
        dict(event)
        for events in (payload.get("events") or {}).values()
        for event in events
        if event.get("event_type") == MiniQMTExecutionEventType.SHADOW_RECONCILIATION_REPORTED.value
    ]


def _seed_gray_single_day_shadow_evidence(
    runtime_repo: JsonFileMiniQMTExecutionRuntimeRepository,
    *,
    runtime_id: str,
    portfolio_id: str,
    strategy_slot_id: str,
    binding_id: str,
    run_id: str,
    plan_id: str,
    trade_date: date = TRADE_DATE,
) -> None:
    MiniQMTShadowReconciler(repository=runtime_repo).reconcile(
        runtime_id=runtime_id,
        scenario=MiniQMTShadowScenario.FULL_FILL,
        a_runtime={
            "runtime_id": f"{runtime_id}_event_loop",
            "runtime_kind": "event_loop",
            "ledger": {"child_orders": [], "trades": [], "cash": {}, "positions": {}},
            "metadata": {"broker_called": False, "broker_mutated": False},
        },
        b_runtime={
            "runtime_id": f"{runtime_id}_compiler",
            "runtime_kind": "compiler",
            "ledger": {"child_orders": [], "trades": [], "cash": {}, "positions": {}},
            "metadata": {"broker_called": False, "broker_mutated": False},
        },
        metadata={
            "trade_date": trade_date.isoformat(),
            "account_group_id": "ag_minqmt_QMT_SIM_ACCOUNT_sim",
            "portfolio_id": portfolio_id,
            "strategy_slot_id": strategy_slot_id,
            "binding_id": binding_id,
            "run_id": run_id,
            "execution_plan_id": plan_id,
            "source": "d4_single_day_smoke_test",
        },
    )


def test_scheduler_miniqmt_shadow_remains_inert_when_disabled_by_default(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    scheduler, repo, broker, qmt_binding = _miniqmt_shadow_test_scheduler()
    shadow_store = tmp_path / "miniqmt-shadow-disabled.json"
    monkeypatch.delenv("MINIQMT_SHADOW_ENABLED", raising=False)
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME_STORE_PATH", str(shadow_store))

    def _a_submit_must_not_run(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("default compiler scope must not route through A event_loop submit")

    monkeypatch.setattr(simulation_bridges.MiniQMTExecutionBridge, "submit_event_loop_plan", _a_submit_must_not_run)

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    assert submitted.submitted_count == 1
    assert submitted.results[0].status == "RECONCILED"
    assert submitted.results[0].run.run_payload_json["broker_called"] is True
    assert "miniqmt_runtime_kind" not in submitted.results[0].run.run_payload_json
    assert "miniqmt_runtime_route" not in submitted.results[0].run.run_payload_json
    assert "miniqmt_shadow_reconciliation" not in submitted.results[0].run.run_payload_json
    assert _runtime_store_shadow_events(shadow_store) == []
    assert len(broker.place_order_payloads) == len(submitted.results[0].execution_plan.intents)
    assert qmt_binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM


def test_scheduler_miniqmt_shadow_persists_durable_evidence_without_touching_broker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    board_lot_candidates = [
        candidate.model_copy(update={"target_quantity": 200})
        if candidate.symbol == "688001.SH"
        else candidate
        for candidate in _candidate_rows()
    ]
    scheduler, repo, broker, qmt_binding = _miniqmt_shadow_test_scheduler(candidates=board_lot_candidates)
    shadow_store = tmp_path / "miniqmt-shadow-enabled.json"
    monkeypatch.setenv("MINIQMT_SHADOW_ENABLED", "true")
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME_STORE_PATH", str(shadow_store))

    observed: dict[str, Any] = {}
    original = simulation_bridges.MiniQMTExecutionBridge.run_shadow_reconciliations

    def _wrapped(self, **kwargs: Any):
        observed["called"] = True
        observed["binding_id"] = kwargs["binding"].binding_id
        observed["plan_id"] = kwargs["plan"].plan_id
        observed["scenarios"] = [scenario.value for scenario in kwargs["scenarios"]]
        return original(self, **kwargs)

    monkeypatch.setattr(simulation_bridges.MiniQMTExecutionBridge, "run_shadow_reconciliations", _wrapped)

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    assert observed["called"] is True
    assert observed["binding_id"] == qmt_binding.binding_id
    expected_scenarios = {
        "full_fill",
        "partial_55_stream",
        "reject",
        "cancel",
        "disconnect",
        "restart_recovery",
    }
    assert set(observed["scenarios"]) == expected_scenarios
    latest_run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    assert latest_run is not None
    shadow = latest_run.run_payload_json["miniqmt_shadow_reconciliation"]
    assert shadow["status"] == "SUCCEEDED"
    assert shadow["reason_code"] == "MINIQMT_SHADOW_RECONCILIATION_REPORTED"
    assert shadow["broker_called"] is False
    assert shadow["broker_mutated"] is False
    assert shadow["b_submit_unaffected"] is True
    assert shadow["report_count"] == 6
    assert set(shadow["covered_scenarios"]) == expected_scenarios
    assert shadow["metadata"]["portfolio_id"] == "portfolio_qmt"
    assert shadow["metadata"]["binding_id"] == qmt_binding.binding_id
    assert shadow["metadata"]["run_id"] == submitted.results[0].run.run_id
    assert shadow["metadata"]["trade_date"] == TRADE_DATE.isoformat()
    assert shadow["metadata"]["execution_plan_id"] == submitted.results[0].execution_plan.plan_id
    assert shadow["metadata"]["account_group_id"] == "ag_minqmt_QMT_SIM_ACCOUNT_sim"
    assert latest_run.run_payload_json["broker_called"] is True
    assert latest_run.run_payload_json["strategy_performance"]["broker_backend"] == "minqmt_sim"
    assert len(broker.place_order_payloads) == len(submitted.results[0].execution_plan.intents)

    runtime_repo = JsonFileMiniQMTExecutionRuntimeRepository(shadow_store)
    runtime = runtime_repo.get_runtime(shadow["runtime_id"])
    assert runtime is not None
    durable_events = [
        event
        for event in runtime_repo.list_events(shadow["runtime_id"])
        if event.event_type == MiniQMTExecutionEventType.SHADOW_RECONCILIATION_REPORTED
    ]
    assert {event.payload["scenario"] for event in durable_events} == expected_scenarios
    assert len(durable_events) == 6
    for event in durable_events:
        metadata = event.payload["metadata"]
        assert metadata["portfolio_id"] == "portfolio_qmt"
        assert metadata["strategy_slot_id"] == qmt_binding.strategy_slot_id
        assert metadata["binding_id"] == qmt_binding.binding_id
        assert metadata["run_id"] == submitted.results[0].run.run_id
        assert metadata["trade_date"] == TRADE_DATE.isoformat()
        assert metadata["execution_plan_id"] == submitted.results[0].execution_plan.plan_id
        assert metadata["account_group_id"] == "ag_minqmt_QMT_SIM_ACCOUNT_sim"
        assert metadata["scenario"] == event.payload["scenario"]
        assert event.payload["a_runtime"]["metadata"]["broker_called"] is False
        assert event.payload["a_runtime"]["metadata"]["broker_mutated"] is False
        assert event.payload["b_runtime"]["metadata"]["broker_called"] is False
        assert event.payload["b_runtime"]["metadata"]["broker_mutated"] is False
    assert runtime.metadata["last_shadow_reconciliation"]["metadata"]["binding_id"] == qmt_binding.binding_id
    evidence = _shadow_evidence_for_scope(
        runtime_repo,
        runtime,
        portfolio_id="portfolio_qmt",
        strategy_slot_id=qmt_binding.strategy_slot_id,
        min_trading_days=1,
        required_scenarios=frozenset(expected_scenarios),
    )
    assert set(evidence.covered_scenarios) == expected_scenarios
    assert evidence.missing_scenarios == []
    assert evidence.scenario_coverage_missing is False


def test_scheduler_miniqmt_gray_event_loop_scope_routes_to_a_runtime_with_broker_quote(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    scheduler, repo, broker, qmt_binding = _miniqmt_shadow_test_scheduler()
    runtime_store = tmp_path / "miniqmt-d4-event-loop.json"
    monkeypatch.delenv("MINIQMT_EXECUTION_RUNTIME", raising=False)
    monkeypatch.delenv("MINIQMT_SHADOW_ENABLED", raising=False)
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME_STORE_PATH", str(runtime_store))
    broker.quotes.update(
        {
            "000001.SZ": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 10.0,
                "ask_price_1": 10.0,
                "ask_volume_1": 5000,
                "bid_price_1": 10.0,
                "bid_volume_1": 5000,
            },
            "688001.SH": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 20.0,
                "ask_price_1": 20.0,
                "ask_volume_1": 5000,
                "bid_price_1": 20.0,
                "bid_volume_1": 5000,
            },
            "000003.SZ": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 8.0,
                "ask_price_1": 8.0,
                "ask_volume_1": 5000,
                "bid_price_1": 8.0,
                "bid_volume_1": 5000,
            },
        }
    )

    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    plan = planned.results[0].execution_plan
    run = planned.results[0].run
    runtime_id = simulation_bridges.MiniQMTExecutionBridge._runtime_id(plan=plan, binding=qmt_binding)
    runtime_repo = JsonFileMiniQMTExecutionRuntimeRepository(runtime_store)
    _seed_gray_single_day_shadow_evidence(
        runtime_repo,
        runtime_id=runtime_id,
        portfolio_id="portfolio_qmt",
        strategy_slot_id=qmt_binding.strategy_slot_id,
        binding_id=qmt_binding.binding_id,
        run_id=run.run_id,
        plan_id=plan.plan_id,
    )
    decision = MiniQMTGraySwitchController(repository=runtime_repo).switch_to_event_loop(
        runtime_id=runtime_id,
        portfolio_id="portfolio_qmt",
        strategy_slot_id=qmt_binding.strategy_slot_id,
        mode="SIM",
        trade_date=TRADE_DATE,
        account_group_id="ag_minqmt_QMT_SIM_ACCOUNT_sim",
        reason="d4_single_day_smoke_route_split",
    )
    assert decision.runtime_kind == MiniQMTExecutionRuntimeKind.EVENT_LOOP

    def _b_submit_must_not_run(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("EVENT_LOOP gray scope must not route through B compiler submit_plan")

    monkeypatch.setattr(simulation_bridges.MiniQMTExecutionBridge, "submit_plan", _b_submit_must_not_run)
    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    latest_run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    assert latest_run.run_payload_json["miniqmt_runtime_kind"] == "event_loop"
    assert latest_run.run_payload_json["miniqmt_runtime_route"] == {
        "route": "A_EVENT_LOOP",
        "runtime_kind": "event_loop",
        "gateway_class": "QmtClientMiniQMTEventLoopGateway",
        "oms_authority": "qmt_strategy_ledger",
        "quote_source": "MINIQMT_REALTIME.broker_quote",
        "reason_code": "MINIQMT_EVENT_LOOP_ROUTE_SELECTED",
    }
    qmt_result = latest_run.run_payload_json["qmt_batch_result"]
    assert qmt_result["runtime_evidence"]["source"] == "simulation_runtime_event_loop_submit"
    runtime_repo = JsonFileMiniQMTExecutionRuntimeRepository(runtime_store)
    event_types = [event.event_type for event in runtime_repo.list_events(runtime_id)]
    assert MiniQMTExecutionEventType.GATEWAY_CONNECTED in event_types
    assert MiniQMTExecutionEventType.BROKER_SYNCED in event_types
    assert MiniQMTExecutionEventType.TICK in event_types
    assert MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED in event_types
    child_orders = runtime_repo.list_child_orders(runtime_id, active_only=False)
    assert child_orders
    assert {child.metadata["gateway_class"] for child in child_orders} == {"QmtClientMiniQMTEventLoopGateway"}
    assert {child.metadata["oms_authority"] for child in child_orders} == {"qmt_strategy_ledger"}
    assert {child.metadata["broker_quote_source"] for child in child_orders} == {"MINIQMT_REALTIME.broker_quote"}
    star_child = next(child for child in child_orders if child.symbol == "688001.SH")
    assert star_child.quantity == 201
    assert any(payload["stock_code"] == "688001.SH" and payload["order_volume"] == 201 for payload in broker.place_order_payloads)
    context = scheduler.context_provider._by_binding_id[qmt_binding.binding_id]  # type: ignore[attr-defined]
    ledger_orders = context.qmt_ledger_repository.list_order_ledger(account_id=qmt_binding.broker_account_id)
    assert len(ledger_orders) >= len(child_orders)
    assert {order.raw_json["qmt_strategy_ledger_authority"] for order in ledger_orders} == {True}


def test_miniqmt_shadow_bridge_requires_explicit_scenario_without_delay_fallback() -> None:
    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        simulation_bridges.MiniQMTExecutionBridge._shadow_scenarios(None, None)

    assert exc_info.value.context["reason_code"] == "MINIQMT_SHADOW_SCENARIO_REQUIRED"


def test_scheduler_miniqmt_shadow_failure_is_loud_and_keeps_b_submit_running(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    scheduler, repo, broker, _qmt_binding = _miniqmt_shadow_test_scheduler()
    shadow_store = tmp_path / "miniqmt-shadow-failure.json"
    monkeypatch.setenv("MINIQMT_SHADOW_ENABLED", "true")
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME_STORE_PATH", str(shadow_store))

    def _raise_shadow(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeConfigInvalidError(
            "forced MiniQMT shadow reconciliation failure",
            context={
                "reason_code": "MINIQMT_SHADOW_TEST_FAILURE",
                "binding_id": kwargs["binding"].binding_id,
                "strategy_id": kwargs["binding"].strategy_id,
                "trade_date": kwargs["plan"].target_trade_date.isoformat(),
            },
        )

    monkeypatch.setattr(simulation_bridges.MiniQMTExecutionBridge, "run_shadow_reconciliations", _raise_shadow)

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    assert submitted.submitted_count == 1
    latest_run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    assert latest_run is not None
    failure = latest_run.run_payload_json["miniqmt_shadow_reconciliation"]
    assert failure["status"] == "FAILED_OBSERVATION_ONLY"
    assert failure["reason_code"] == "MINIQMT_SHADOW_TEST_FAILURE"
    assert failure["b_submit_unaffected"] is True
    assert failure["broker_called"] is False
    assert failure["broker_mutated"] is False
    assert latest_run.run_payload_json["broker_called"] is True
    assert latest_run.run_payload_json["simulation_alerts"][-1]["reason_code"] == "MINIQMT_SHADOW_TEST_FAILURE"
    assert _runtime_store_shadow_events(shadow_store) == []
    assert len(broker.place_order_payloads) == len(submitted.results[0].execution_plan.intents)


def test_scheduler_miniqmt_shadow_does_not_activate_for_local_sim_bindings(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    paper_repo = InMemoryPaperTradingV2Repository()
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _local_sim_context_with_real_broker(
                    portfolio_id="portfolio_shared",
                    release=release,
                    paper_repository=paper_repo,
                )
            }
        ),
    )
    shadow_store = tmp_path / "miniqmt-shadow-local.json"
    monkeypatch.setenv("MINIQMT_SHADOW_ENABLED", "true")
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME_STORE_PATH", str(shadow_store))

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    assert submitted.submitted_count == 1
    assert submitted.results[0].status == "SUBMITTED"
    assert "miniqmt_shadow_reconciliation" not in submitted.results[0].run.run_payload_json
    assert _runtime_store_shadow_events(shadow_store) == []
    assert submitted.results[0].run.run_payload_json["broker_called"] is True


def _qmt_account(repo: InMemoryQmtStrategyLedgerRepository, *, account_id: str, strategy_name: str) -> None:
    repo.create_virtual_account(
        VirtualAccount(
            strategy_id=f"strategy_{strategy_name}",
            strategy_name=strategy_name,
            display_name=strategy_name,
            account_id=account_id,
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )


def _managed_buy_request(*, account_id: str, strategy_name: str, order_remark: str) -> ManagedOrderRequest:
    return ManagedOrderRequest(
        account_id=account_id,
        strategy_name=strategy_name,
        symbol="000001.SZ",
        side="BUY",
        order_type=BUY_ORDER_TYPE,
        quantity=100,
        price_type=5,
        price=Decimal("10"),
        order_remark=order_remark,
        trade_date=TRADE_DATE,
        mode="SIM",
    )


def test_miniqmt_managed_order_disconnect_freezes_until_reconnect_reconcile() -> None:
    account_id = "QMT_DISCONNECT_ACCOUNT"
    strategy_name = "DisconnectFreezeStrategy"
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    _qmt_account(qmt_repo, account_id=account_id, strategy_name=strategy_name)
    broker = FakeManagedOrderBroker(fail_next_place=True, connect_ok=False)
    service = QmtManagedOrderService(
        repository=qmt_repo,
        broker=broker,  # type: ignore[arg-type]
        calendar_provider=StaticTradingCalendarProvider([TRADE_DATE]),
    )

    first = service.submit_batch(
        [_managed_buy_request(account_id=account_id, strategy_name=strategy_name, order_remark="freeze-first")]
    )

    assert first.success is False
    assert first.results[0].broker_called is True
    assert first.results[0].preflight.primary_error.code == "MINIQMT_BROKER_DISCONNECTED_FREEZE"
    assert service.broker_disconnect_freeze_status()["frozen"] is True
    assert len(broker.place_order_payloads) == 1

    frozen = service.submit_batch(
        [_managed_buy_request(account_id=account_id, strategy_name=strategy_name, order_remark="freeze-blocked")]
    )

    assert frozen.success is False
    assert frozen.results[0].broker_called is False
    assert frozen.results[0].preflight.primary_error.code == "MINIQMT_BROKER_DISCONNECTED_FREEZE"
    assert frozen.results[0].preflight.primary_error.context["recovery"]["stage"] == "BROKER_STILL_DISCONNECTED"
    assert len(broker.place_order_payloads) == 1

    broker.connect_ok = True
    recovered = service.submit_batch(
        [_managed_buy_request(account_id=account_id, strategy_name=strategy_name, order_remark="freeze-recovered")]
    )

    assert recovered.success is True
    assert recovered.results[0].broker_called is True
    assert broker.order_query_calls >= 1
    assert broker.trade_query_calls >= 1
    assert len(broker.place_order_payloads) == 2
    status = service.broker_disconnect_freeze_status()
    assert status["frozen"] is False
    assert status["last_recovery"]["reason_code"] == "MINIQMT_BROKER_RECONNECTED_RECONCILED"


def test_miniqmt_managed_order_reconnect_reconcile_failure_keeps_freeze_without_submit() -> None:
    account_id = "QMT_RECONNECT_FAIL_ACCOUNT"
    strategy_name = "DisconnectFreezeReconcileFailStrategy"
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    _qmt_account(qmt_repo, account_id=account_id, strategy_name=strategy_name)
    broker = FakeManagedOrderBroker(fail_next_place=True, connect_ok=True, fail_order_query=True)
    service = QmtManagedOrderService(
        repository=qmt_repo,
        broker=broker,  # type: ignore[arg-type]
        calendar_provider=StaticTradingCalendarProvider([TRADE_DATE]),
    )

    first = service.submit_batch(
        [_managed_buy_request(account_id=account_id, strategy_name=strategy_name, order_remark="freeze-first")]
    )
    assert first.success is False
    assert first.results[0].broker_called is True
    assert first.results[0].preflight.primary_error.code == "MINIQMT_BROKER_DISCONNECTED_FREEZE"

    frozen = service.submit_batch(
        [_managed_buy_request(account_id=account_id, strategy_name=strategy_name, order_remark="freeze-blocked")]
    )

    assert frozen.success is False
    assert frozen.results[0].broker_called is False
    error = frozen.results[0].preflight.primary_error
    assert error.code == "MINIQMT_BROKER_RECONNECT_RECONCILE_FAILED"
    assert error.context["recovery"]["stage"] == "RECONNECT_RECONCILE_FAILED"
    assert service.broker_disconnect_freeze_status()["frozen"] is True
    assert broker.order_query_calls == 1
    assert broker.trade_query_calls == 0
    assert len(broker.place_order_payloads) == 1


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
        self.suspend_status_provider = self
        self.st_status_provider = self

    def get_suspend_status(self, symbol: str, trade_date: date):
        return SimpleNamespace(
            symbol=symbol,
            trade_date=trade_date,
            is_suspended=False,
            suspend_type=None,
            suspend_timing=None,
            source="unit_test.suspend_status",
        )

    def get_st_status(self, symbol: str, trade_date: date):
        return DailyStStatus(
            symbol=symbol,
            trade_date=trade_date,
            is_st=False,
            source="unit_test.stock_st",
        )

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


@pytest.mark.parametrize(
    ("exc_cls", "reason_code"),
    (
        (DataUnavailableError, "UNIT_PRE_RUN_CONTEXT_UNAVAILABLE"),
        (RuntimeConfigInvalidError, "UNIT_PRE_RUN_CONFIG_INVALID"),
    ),
)
def test_scheduler_persists_pre_run_binding_failure_without_duplicate_rows(exc_cls, reason_code) -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None

    def exc_factory(binding, trade_date):
        return exc_cls(
            "unit pre-run failure",
            context={
                "reason_code": reason_code,
                "binding_id": binding.binding_id,
                "strategy_id": binding.strategy_id,
                "trade_date": trade_date.isoformat(),
            },
        )

    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=SelectiveFailingContextProvider(
            failing_binding_id=local_binding.binding_id,
            exc_factory=exc_factory,
        ),
    )

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    second = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )

    runs = [
        run
        for run in repo.list_simulation_daily_runs(trade_date=TRADE_DATE, limit=10)
        if run.binding_id == local_binding.binding_id
    ]
    latest = repo.get_simulation_daily_run_by_key(
        strategy_id=local_binding.strategy_id,
        binding_id=local_binding.binding_id,
        trade_date=TRADE_DATE,
    )
    assert latest is not None
    pre_run_failure = latest.run_payload_json["pre_run_failure"]
    detail = SimulationRuntimeOpsService(repository=repo).get_run_detail(latest.run_id)

    assert first.failed_count == 1
    assert first.results[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert first.results[0].run is not None
    assert second.failed_count == 1
    assert second.results[0].run is not None
    assert second.results[0].run.run_id == first.results[0].run.run_id
    assert len(runs) == 1
    assert latest.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert latest.execution_plan_id is None
    assert pre_run_failure["stage"] == "PRE_RUN_FAILED"
    assert pre_run_failure["reason_code"] == reason_code
    assert pre_run_failure["binding_id"] == local_binding.binding_id
    assert pre_run_failure["strategy_id"] == local_binding.strategy_id
    assert pre_run_failure["trade_date"] == TRADE_DATE.isoformat()
    assert pre_run_failure["broker_called"] is False
    assert pre_run_failure["observed_count"] == 2
    assert latest.run_payload_json["broker_called"] is False
    assert latest.run_payload_json["submitted_intents"] == 0
    assert latest.run_payload_json["failed_intents"] == 0
    assert latest.run_payload_json["submit_failure"]["stage"] == "PRE_RUN_FAILED"
    assert detail["run"]["errors"][0]["code"] == "PRE_RUN_FAILED"
    assert detail["run"]["errors"][0]["context"]["reason_code"] == reason_code


def test_scheduler_pre_run_binding_failure_does_not_block_other_bindings() -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None

    def exc_factory(binding, trade_date):
        return DataUnavailableError(
            "unit LocalSim context unavailable",
            context={
                "reason_code": "UNIT_PRE_RUN_CONTEXT_UNAVAILABLE",
                "binding_id": binding.binding_id,
                "trade_date": trade_date.isoformat(),
            },
        )

    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=SelectiveFailingContextProvider(
            failing_binding_id=local_binding.binding_id,
            exc_factory=exc_factory,
            by_binding_id={
                qmt_binding.binding_id: _position_context(portfolio_id="portfolio_qmt_still_runs"),
            },
        ),
    )

    result = scheduler.run_once(trade_date=TRADE_DATE, data_source="DB_HISTORICAL", submit=False)
    by_binding_id = {item.binding_id: item for item in result.results}
    failed_run = repo.get_simulation_daily_run_by_key(
        strategy_id=local_binding.strategy_id,
        binding_id=local_binding.binding_id,
        trade_date=TRADE_DATE,
    )
    qmt_run = repo.get_simulation_daily_run_by_key(
        strategy_id=qmt_binding.strategy_id,
        binding_id=qmt_binding.binding_id,
        trade_date=TRADE_DATE,
    )

    assert result.total_bindings == 2
    assert result.failed_count == 1
    assert result.planned_count == 1
    assert by_binding_id[local_binding.binding_id].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert by_binding_id[local_binding.binding_id].error["context"]["reason_code"] == "UNIT_PRE_RUN_CONTEXT_UNAVAILABLE"
    assert by_binding_id[qmt_binding.binding_id].status == "PLANNED"
    assert failed_run is not None
    assert failed_run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert qmt_run is not None
    assert qmt_run.execution_plan_id is not None


def test_scheduler_isolates_live_inference_preflight_failure_and_continues_later_bindings() -> None:
    repo = InMemorySimulationRuntimeRepository()
    service = StrategyRuntimeReleaseService(repository=repo)
    good_release = _create_scheduler_release(repo, package_id="pkg_good", manifest_sha256="manifest_good")
    bad_release = _create_scheduler_release(repo, package_id="pkg_bad", manifest_sha256="manifest_bad")
    good_binding = service.create_binding(
        strategy_id="strategy_good_after_bad",
        release=good_release,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        capital_allocation=100_000,
        approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
        created_by="unit-test",
        created_reason="good binding processed after bad preflight",
    )
    bad_binding = service.create_binding(
        strategy_id="strategy_bad_preflight",
        release=bad_release,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        capital_allocation=100_000,
        approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
        created_by="unit-test",
        created_reason="bad binding with live inference preflight failure",
    )
    selection = PackageRoutingSelectionService(
        {good_release.package_id: good_release, bad_release.package_id: bad_release},
        failing_package_id=bad_release.package_id,
    )
    context_provider = StaticSimulationRunContextProvider(
        by_binding_id={
            bad_binding.binding_id: _position_context(portfolio_id="portfolio_bad"),
            good_binding.binding_id: _position_context(portfolio_id="portfolio_good"),
        }
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=selection,
        context_provider=context_provider,
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    by_binding_id = {item.binding_id: item for item in result.results}
    failed_run = repo.get_simulation_daily_run_by_key(
        strategy_id=bad_binding.strategy_id,
        binding_id=bad_binding.binding_id,
        trade_date=TRADE_DATE,
    )
    good_run = repo.get_simulation_daily_run_by_key(
        strategy_id=good_binding.strategy_id,
        binding_id=good_binding.binding_id,
        trade_date=TRADE_DATE,
    )

    assert result.total_bindings == 2
    assert result.failed_count == 1
    assert result.planned_count == 1
    assert [call["package_ids"][0] for call in selection.calls] == ["pkg_bad", "pkg_good"]
    assert by_binding_id[bad_binding.binding_id].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert by_binding_id[good_binding.binding_id].status == "PLANNED"
    assert failed_run is not None
    assert failed_run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert failed_run.execution_plan_id is None
    assert failed_run.run_payload_json["broker_called"] is False
    assert failed_run.run_payload_json["submitted_intents"] == 0
    assert good_run is not None
    assert good_run.execution_plan_id is not None
    pre_run_failure = failed_run.run_payload_json["pre_run_failure"]
    assert pre_run_failure["stage"] == "preflight"
    assert pre_run_failure["failure_stage"] == "preflight"
    assert pre_run_failure["reason_code"] == "strategy_package_model_code_missing"
    assert pre_run_failure["package_id"] == bad_release.package_id
    assert pre_run_failure["manifest_sha256"] == bad_release.manifest_sha256
    assert pre_run_failure["blocked_check"] == PREFLIGHT_CHECK_MODEL_PARAMS
    assert pre_run_failure["missing_relative_paths"] == ["model.py"]
    assert pre_run_failure["broker_called"] is False
    assert pre_run_failure["submitted_intents"] == 0
    assert by_binding_id[bad_binding.binding_id].error["context"]["reason_code"] == "strategy_package_model_code_missing"
    assert by_binding_id[bad_binding.binding_id].error["context"]["blocked_check"] == PREFLIGHT_CHECK_MODEL_PARAMS
    detail = SimulationRuntimeOpsService(repository=repo).get_run_detail(failed_run.run_id)
    assert detail["run"]["errors"][0]["code"] == "PRE_RUN_FAILED"
    assert detail["run"]["errors"][0]["context"]["reason_code"] == "strategy_package_model_code_missing"


def test_scheduler_raise_on_error_reraises_live_inference_preflight_failure() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    selection = PackageRoutingSelectionService(
        {release.package_id: release},
        failing_package_id=release.package_id,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=selection,
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_raise")}
        ),
    )

    with pytest.raises(LiveInferencePreflightError):
        scheduler.run_once(
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            submit=False,
            raise_on_error=True,
        )

    assert (
        repo.get_simulation_daily_run_by_key(
            strategy_id=local_binding.strategy_id,
            binding_id=local_binding.binding_id,
            trade_date=TRADE_DATE,
        )
        is None
    )


def test_scheduler_does_not_swallow_system_exit_from_binding_boundary() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None

    class SystemExitSelectionService:
        def run_selection(self, **_kwargs):
            raise SystemExit("fatal scheduler stop signal")

    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=SystemExitSelectionService(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_system_exit")}
        ),
    )

    with pytest.raises(SystemExit):
        scheduler.run_once(
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            submit=False,
        )

    assert (
        repo.get_simulation_daily_run_by_key(
            strategy_id=local_binding.strategy_id,
            binding_id=local_binding.binding_id,
            trade_date=TRADE_DATE,
        )
        is None
    )


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
    assert "succeeded_with_capacity_residual" not in latest_run.run_payload_json
    assert "miniqmt_capacity_residual_observability" not in latest_run.run_payload_json
    assert "succeeded_with_capacity_residual" not in latest_run.run_payload_json["strategy_performance"]
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
    assert reconciliation["submit_result_gate"]["succeeded_with_capacity_residual"] is True
    assert reconciliation["qmt_batch_residual_summary"]["capacity_residual_count"] == 1
    observability = failed_run.run_payload_json["miniqmt_capacity_residual_observability"]
    assert failed_run.run_payload_json["succeeded_with_capacity_residual"] is True
    assert failed_run.run_payload_json["capacity_residual_count"] == 1
    assert failed_run.run_payload_json["capacity_residual_failed_intents"] == 1
    assert observability["succeeded_with_capacity_residual"] is True
    assert observability["failed_intents"] == 1
    assert observability["capacity_residual_count"] == 1
    assert observability["alert"]["reason_code"] == "MINIQMT_SUCCEEDED_WITH_CAPACITY_RESIDUAL"
    assert failed_run.run_payload_json["simulation_alerts"][0]["reason_code"] == "MINIQMT_SUCCEEDED_WITH_CAPACITY_RESIDUAL"
    performance = failed_run.run_payload_json["strategy_performance"]
    assert performance["succeeded_with_capacity_residual"] is True
    assert performance["capacity_residual_count"] == 1
    assert performance["capacity_residual_failed_intents"] == 1
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
    assert recovered_run.run_payload_json["succeeded_with_capacity_residual"] is True
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
    assert terminalization["miniqmt_capacity_residual_observability"]["succeeded_with_capacity_residual"] is True
    assert latest.run_payload_json["succeeded_with_capacity_residual"] is True
    assert latest.run_payload_json["capacity_residual_count"] == 1
    assert latest.run_payload_json["capacity_residual_failed_intents"] == 1
    assert post_close.stale_run_results[0]["succeeded_with_capacity_residual"] is True
    assert post_close.stale_run_results[0]["alert"]["reason_code"] == "MINIQMT_SUCCEEDED_WITH_CAPACITY_RESIDUAL"
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


def test_scheduler_rejects_side_effect_free_failed_retry_outside_shared_window_without_broker_call() -> None:
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
            lot_id="lot_scheduler_qmt_retry_window_gate",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_retry_window_gate",
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
    assert failed_run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert failed_run.run_payload_json["broker_called"] is False
    assert broker.place_order_payloads == []

    retried = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 16, 33),
    )
    retried_run = repo.get_simulation_daily_run(failed_run.run_id)
    gate = retried_run.run_payload_json["submit_window_gate"]

    assert retried.results[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert retried_run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert retried_run.run_payload_json["rebuilt_after_side_effect_free_failure"] is True
    assert gate["reason_code"] == MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW
    assert gate["broker_called_before_rejection"] is False
    assert retried_run.run_payload_json["broker_called"] is False
    assert broker.place_order_payloads == []


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


def test_scheduler_rejects_fresh_miniqmt_submit_outside_shared_window_without_broker_call() -> None:
    scheduler, repo, broker, _qmt_binding = _miniqmt_shadow_test_scheduler()

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 16, 33),
    )
    run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    gate = run.run_payload_json["submit_window_gate"]

    assert result.results[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert gate["reason_code"] == MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW
    assert gate["active_window"] is None
    blocked_plan = repo.get_execution_plan(run.execution_plan_id)
    assert gate["blocked_intent_count"] == len(blocked_plan.intents)
    assert gate["schedule_windows"] == list(compute_schedule_windows(trade_date=TRADE_DATE, as_of_time=datetime(2026, 5, 21, 16, 33)))
    assert run.run_payload_json["submit_failure"]["stage"] == MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW
    assert run.run_payload_json["broker_called"] is False
    assert broker.place_order_payloads == []


def test_scheduler_rejects_fresh_localsim_submit_outside_shared_window_without_broker_call() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    broker = FakeLocalSimBroker()
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _position_context(
                    portfolio_id="portfolio_local_window_gate",
                    local_broker=broker,
                )
            }
        ),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 16, 33),
    )
    run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    gate = run.run_payload_json["submit_window_gate"]

    assert result.results[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert gate["reason_code"] == MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW
    assert gate["broker_backend"] == SimulationBrokerBackend.LOCAL_SIM.value
    assert run.run_payload_json["broker_called"] is False
    assert broker.submitted == []


def test_scheduler_allows_miniqmt_submit_inside_shared_window() -> None:
    scheduler, _repo, broker, _qmt_binding = _miniqmt_shadow_test_scheduler()

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 10, 0),
    )

    assert result.results[0].status == "RECONCILED"
    assert result.results[0].run.status == SimulationDailyRunStatus.SUCCEEDED
    assert broker.place_order_payloads


def test_scheduler_rejects_deferred_dependent_buy_replay_after_close_without_duplicate_buy() -> None:
    scheduler, repo, broker, qmt_binding, qmt_repo, _snapshot_client = _miniqmt_scheduler_with_ledger_context(
        cash=Decimal("3500")
    )

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 10, 0),
    )
    first_run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    assert [payload["order_type"] for payload in broker.place_order_payloads] == [SELL_ORDER_TYPE]

    account_after_sell = qmt_repo.get_virtual_account(qmt_binding.strategy_id)
    qmt_repo.update_virtual_account(replace(account_after_sell, cash=Decimal("100000")))
    second = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 16, 33),
    )
    blocked = repo.get_simulation_daily_run(first_run.run_id)
    gate = blocked.run_payload_json["submit_window_gate"]

    assert second.results[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert blocked.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert gate["reason_code"] == MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW
    assert gate["broker_called_before_rejection"] is True
    assert blocked.run_payload_json["broker_called"] is True
    assert [payload["order_type"] for payload in broker.place_order_payloads] == [SELL_ORDER_TYPE]
    batch = qmt_repo.get_order_batch(first_run.run_payload_json["qmt_batch_id"])
    assert batch is not None
    assert batch.batch_status == OrderBatchStatus.PARTIAL
    assert batch.metadata["dependent_buy_deferred"] is True


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


def test_scheduler_cross_day_terminalizes_side_effect_miniqmt_open_order_with_fresh_broker_reconcile() -> None:
    scheduler, repo, broker, _qmt_binding, qmt_repo, _snapshot_client = _miniqmt_scheduler_with_ledger_context()

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 10, 0),
    )
    run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
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
            qmt_order_id="900000565",
            symbol=accepted_intent.symbol,
            order_type=accepted_intent.order_type,
            order_volume=accepted_intent.quantity,
            traded_volume=0,
            order_status=STATUS_OPEN_LIKE,
            account_id=accepted_intent.account_id,
            trade_date=accepted_intent.trade_date,
            price_type=accepted_intent.price_type,
            price=Decimal("8.0"),
            status_msg="cross-day open order remains at broker",
            order_remark=accepted_intent.order_remark,
        )
    )
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)

    today = scheduler.run_once(
        trade_date=TRADE_DATE + timedelta(days=1),
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 22, 10, 0),
    )
    latest = repo.get_simulation_daily_run(run.run_id)
    terminalization = latest.run_payload_json["miniqmt_post_close_terminalization"]

    assert today.stale_terminalized_count == 1
    assert today.stale_run_results[0]["cross_day_terminalization"] is True
    assert today.stale_run_results[0]["status"] == SimulationDailyRunStatus.FAILED_TERMINAL.value
    assert latest.status == SimulationDailyRunStatus.FAILED_TERMINAL
    assert terminalization["reason"] == "miniqmt_post_close_open_orders_terminal_failed"
    assert terminalization["open_order_evidence"]["open_order_count"] == 1
    assert terminalization["fresh_reconcile"]["source"] == "qmt_broker_snapshot_and_strategy_ledger"
    assert len(broker.place_order_payloads) == len(submitted.results[0].execution_plan.intents)


def test_scheduler_cross_day_terminalizes_side_effect_miniqmt_succeeded_batch_after_fresh_reconcile() -> None:
    scheduler, repo, _broker, _qmt_binding, _qmt_repo, _snapshot_client = _miniqmt_scheduler_with_ledger_context()

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 10, 0),
    )
    run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)

    today = scheduler.run_once(
        trade_date=TRADE_DATE + timedelta(days=1),
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 22, 10, 0),
    )
    latest = repo.get_simulation_daily_run(run.run_id)
    terminalization = latest.run_payload_json["miniqmt_post_close_terminalization"]

    assert today.stale_terminalized_count == 1
    assert today.stale_run_results[0]["cross_day_terminalization"] is True
    assert today.stale_run_results[0]["status"] == SimulationDailyRunStatus.SUCCEEDED.value
    assert latest.status == SimulationDailyRunStatus.SUCCEEDED
    assert terminalization["reason"] == "miniqmt_post_close_batch_succeeded"
    assert terminalization["fresh_reconcile"]["source"] == "qmt_broker_snapshot_and_strategy_ledger"


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


def test_background_scheduler_result_surfaces_miniqmt_capacity_residual_alert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ResidualLifecycleScheduler:
        def status(self) -> dict[str, Any]:
            return {"ok": True, "scheduler": "residual_lifecycle_scheduler"}

        def run_once(self, **kwargs: Any) -> SimulationSchedulerRunOnceResult:
            run = SimulationDailyRun(
                run_id="simrun_background_capacity_residual",
                trade_date=TRADE_DATE,
                strategy_id="strategy_background_qmt",
                broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
                package_id="pkg_background",
                manifest_sha256="manifest_background",
                release_id="srr_background",
                release_hash="release_hash_background",
                binding_id="simbind_background",
                binding_hash="binding_hash_background",
                account_group_id="ag_background",
                strategy_slot_id="slot_background",
                status=SimulationDailyRunStatus.SUCCEEDED,
                run_payload_json={
                    "last_stage": "SUCCEEDED",
                    "broker_called": True,
                    "failed_intents": 3,
                    "qmt_batch_id": "batch_background_capacity",
                    "qmt_batch_status": "PARTIAL",
                    "qmt_batch_result": {
                        "batch_status": "PARTIAL",
                        "failed": 3,
                        "results": [
                            {
                                "success": False,
                                "broker_called": False,
                                "preflight": {
                                    "primary_error_code": "SKIPPED_INSUFFICIENT_CAPITAL",
                                    "primary_error": {"message": "capacity residual"},
                                    "errors": [
                                        {
                                            "code": "SKIPPED_INSUFFICIENT_CAPITAL",
                                            "message": "capacity residual",
                                            "context": {},
                                        }
                                    ],
                                },
                            }
                        ],
                    },
                    "reconcile_after_submit": {
                        "submit_result_gate": {
                            "schema_version": "miniqmt_reconcile_submit_result_gate_v2",
                            "status": "SUCCEEDED",
                            "reason": "miniqmt_capacity_residual_skipped_and_reconciled",
                            "terminal_capacity_residual": True,
                        }
                    },
                },
            )
            return SimulationSchedulerRunOnceResult(
                trade_date=TRADE_DATE,
                data_source="DB_HISTORICAL",
                submit=True,
                total_bindings=1,
                results=(
                    SimulationSchedulerBindingResult(
                        binding_id=run.binding_id,
                        strategy_id=run.strategy_id,
                        broker_backend=run.broker_backend,
                        status="RECONCILED",
                        run=run,
                        data_source="MINIQMT_REALTIME",
                    ),
                ),
                as_of_time=kwargs.get("as_of_time"),
            )

        def post_close_reconcile_once(self, **kwargs: Any) -> SimulationSchedulerRunOnceResult:
            raise AssertionError("execution window should call run_once")

    monkeypatch.setenv("SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT", "true")
    background = SimulationLifecycleBackgroundScheduler(
        lifecycle_scheduler=ResidualLifecycleScheduler(),  # type: ignore[arg-type]
        trading_calendar_service=StaticTradingCalendarProvider([TRADE_DATE]),
    )

    result = background.run_once(as_of_time=datetime(2026, 5, 21, 2, 0, tzinfo=UTC))

    assert result["submit"] is True
    assert result["summary"]["succeeded_with_capacity_residual_count"] == 1
    assert result["summary"]["capacity_residual_count"] == 1
    assert result["summary"]["capacity_residual_failed_intents"] == 3
    assert result["processed"][0]["succeeded_with_capacity_residual"] is True
    assert result["processed"][0]["capacity_residual_failed_intents"] == 3
    assert result["alerts"][0]["reason_code"] == "MINIQMT_SUCCEEDED_WITH_CAPACITY_RESIDUAL"


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


def test_scheduler_terminalizes_deterministic_localsim_board_lot_rejection_without_replay() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    broker = BoardLotRejectingLocalSimBroker()
    position = PositionLot(
        portfolio_id="portfolio_board_lot_terminal",
        symbol="688720.SH",
        quantity=1547,
        available_quantity=1547,
        avg_cost=10.0,
        trade_date=TRADE_DATE - timedelta(days=1),
    )
    context = SimulationRunContext(
        portfolio_id="portfolio_board_lot_terminal",
        current_positions={"688720.SH": position},
        current_prices={"688720.SH": 10.0},
        local_broker=broker,
        cash=100_000.0,
        market_data_source=MinuteDataSource.DB_HISTORICAL.value,
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
    assert result.results[0].status == SimulationDailyRunStatus.FAILED_TERMINAL.value
    assert latest_run.status == SimulationDailyRunStatus.FAILED_TERMINAL
    assert latest_run.run_payload_json["broker_called"] is False
    assert latest_run.run_payload_json["submitted_intents"] == 0
    assert latest_run.run_payload_json["failed_intents"] == 1
    diagnostics = latest_run.run_payload_json["local_sim_deterministic_submit_failure"]
    assert diagnostics["reason_code"] == "LOCAL_SIM_BOARD_LOT_VIOLATION"
    assert diagnostics["stage"] == "LOCAL_SIM_SUBMIT_FAILED"
    assert len(broker.submitted) == 1

    second = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    assert second.results[0].status == "REUSED_EXISTING_PLAN"
    assert len(broker.submitted) == 1


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


def test_production_context_provider_uses_miniqmt_quote_for_pre_trade_gate_today() -> None:
    """MiniQMT same-day gate must use broker quotes and must not call the TDX provider."""
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
    tradability = FakePreTradeTradabilityProvider()
    broker = FakeManagedOrderBroker(
        positions=[{"stock_code": "688689.SH", "quantity": 878, "can_sell": 878}],
        quotes={
            "688689.SH": {
                "bidPrice": [0.0],
                "askPrice": [0.0],
                "bidVol": [0],
                "askVol": [0],
                "lastPrice": 46.82,
                "lastClose": 46.8,
                "openPrice": 0.0,
                "highPrice": 0.0,
                "lowPrice": 0.0,
                "volume": 0,
                "amount": 0,
                "time": datetime.now().strftime("%Y%m%d%H%M%S"),
            }
        },
    )
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
    assert ctx.pre_trade_tradability["688689.SH"]["source"] == "MINIQMT_REALTIME.broker_quote"
    assert ctx.context_diagnostics["pre_trade_tradability"]["blocked_symbols"] == [
        {"symbol": "688689.SH", "reason_code": "NO_TRADABLE_REALTIME_QUOTE", "source": "MINIQMT_REALTIME.broker_quote"}
    ]
    assert tradability.calls == []
    assert broker.full_tick_calls == [["688689.SH"]]


def test_production_context_provider_miniqmt_quote_yuan_limits_do_not_fail_pre_run() -> None:
    """MiniQMT broker quotes are yuan-denominated and must not use TDX raw-li rounding."""
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
            lot_id="lot_miniqmt_quote",
            strategy_id="strat1",
            symbol="603303.SH",
            open_trade_id="trade_miniqmt_quote",
            open_date=date.today() - timedelta(days=1),
            quantity=100,
            available_quantity=100,
            remaining_quantity=100,
            avg_cost=Decimal("30.14"),
            cost_amount=Decimal("3014.00"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    tradability = FakePreTradeTradabilityProvider()
    broker = FakeManagedOrderBroker(
        positions=[{"stock_code": "603303.SH", "quantity": 100, "can_sell": 100}],
        quotes={
            "603303.SH": {
                "bidPrice": [30.23],
                "askPrice": [30.26],
                "bidVol": [108],
                "askVol": [17],
                "lastPrice": 30.23,
                "lastClose": 30.14,
                "openPrice": 30.27,
                "highPrice": 31.8,
                "lowPrice": 29.01,
                "volume": 66974,
                "amount": 203373536,
                "time": datetime.now().strftime("%Y%m%d%H%M%S"),
            }
        },
    )
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 30.23 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        pre_trade_tradability_provider=tradability,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())

    status = ctx.pre_trade_tradability["603303.SH"]
    assert status["is_tradable"] is True
    assert status["quote_evidence"]["quote_price_basis"] == "yuan"
    assert status["quote_evidence"]["limit_up"] == pytest.approx(33.15)
    assert status["quote_evidence"]["limit_down"] == pytest.approx(27.13)
    assert tradability.calls == []
    assert broker.full_tick_calls == [["603303.SH"]]


def test_production_context_provider_miniqmt_degenerate_raw_limit_metadata_passes_pre_run() -> None:
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
            lot_id="lot_miniqmt_degenerate_limit_metadata",
            strategy_id="strat1",
            symbol="000048.SZ",
            open_trade_id="trade_miniqmt_degenerate_limit_metadata",
            open_date=date.today() - timedelta(days=1),
            quantity=100,
            available_quantity=100,
            remaining_quantity=100,
            avg_cost=Decimal("20.75"),
            cost_amount=Decimal("2075.00"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    tradability = FakePreTradeTradabilityProvider()
    broker = FakeManagedOrderBroker(
        positions=[{"stock_code": "000048.SZ", "quantity": 100, "can_sell": 100}],
        quotes={
            "000048.SZ": {
                "price_basis": "raw_li",
                "bidPrice": [20.65],
                "askPrice": [20.66],
                "bidVol": [100],
                "askVol": [100],
                "lastPrice": 20.66,
                "lastClose": 20.75,
                "openPrice": 20.7,
                "highPrice": 20.88,
                "lowPrice": 20.3,
                "limit_up": 20.0,
                "limit_down": 20.0,
                "volume": 12345,
                "amount": 25432100,
                "time": datetime.now().strftime("%Y%m%d%H%M%S"),
            }
        },
    )
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 20.66 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        pre_trade_tradability_provider=tradability,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())

    status = ctx.pre_trade_tradability["000048.SZ"]
    assert status["is_tradable"] is True
    assert status["quote_evidence"]["quote_source"] == "MINIQMT_REALTIME.broker_quote"
    assert status["quote_evidence"]["quote_price_basis"] == "yuan"
    assert status["quote_evidence"]["limit_up"] == pytest.approx(22.83)
    assert status["quote_evidence"]["limit_down"] == pytest.approx(18.68)
    assert broker.full_tick_calls == [["000048.SZ"]]


def test_production_context_provider_miniqmt_quote_missing_is_visible_without_tdx_fallback() -> None:
    """MiniQMT quote outages must surface as MiniQMT evidence, not TDX timestamp failures."""
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
            lot_id="lot_quote_missing",
            strategy_id="strat1",
            symbol="000001.SZ",
            open_trade_id="trade_quote_missing",
            open_date=date.today() - timedelta(days=1),
            quantity=100,
            available_quantity=100,
            remaining_quantity=100,
            avg_cost=Decimal("10.00"),
            cost_amount=Decimal("1000.00"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    tradability = FakePreTradeTradabilityProvider()
    broker = FakeManagedOrderBroker(
        positions=[{"stock_code": "000001.SZ", "quantity": 100, "can_sell": 100}],
        quotes={},
    )
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 10.0 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        pre_trade_tradability_provider=tradability,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())

    status = ctx.pre_trade_tradability["000001.SZ"]
    assert status["is_tradable"] is False
    assert status["reason_code"] == "REALTIME_QUOTE_MISSING"
    assert status["source"] == "MINIQMT_REALTIME.broker_quote"
    assert status["quote_evidence"] == {
        "quote_source": "MINIQMT_REALTIME.broker_quote",
        "quote_present": False,
    }
    assert tradability.calls == []
    assert broker.full_tick_calls == [["000001.SZ"]]


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


def test_scheduler_converts_no_side_effect_reconciling_after_runtime_only_cleanup_and_retries() -> None:
    scheduler, repo, broker, qmt_binding = _miniqmt_shadow_test_scheduler()
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    run = planned.results[0].run
    plan = planned.results[0].execution_plan
    assert run is not None
    assert plan is not None
    runtime_id = simulation_bridges.MiniQMTExecutionBridge._runtime_id(plan=plan, binding=qmt_binding)
    runtime_repo = InMemoryMiniQMTExecutionRuntimeRepository()
    gateway = FakeMiniQMTGateway()
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id=runtime_id,
            account_group_id=qmt_binding.account_group_id or qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            trade_date=TRADE_DATE,
            runtime_config_hash=plan.plan_hash,
        ),
        repository=runtime_repo,
        gateway=gateway,
    )
    runtime.start()
    stale_algo = runtime.create_algo_instance(
        parent_intent_id="intent_stale_reconciling",
        strategy_slot_id=qmt_binding.strategy_slot_id or qmt_binding.binding_id,
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    runtime.submit_child_order(algo_instance_id=stale_algo.algo_instance_id, quantity=100, price=10.0)
    gateway._orders.clear()
    stuck = repo.update_simulation_daily_run(
        run.run_id,
        status=SimulationDailyRunStatus.RECONCILING,
        payload_patch={
            "last_stage": "RECONCILING",
            "broker_called": False,
            "submitted_intents": 0,
            "failed_intents": 0,
            "order_intent_count": len(plan.intents),
        },
    )

    retry_before_recovery = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    assert retry_before_recovery.results[0].status == "REUSED_EXISTING_PLAN"
    assert broker.place_order_payloads == []

    operator_result = runtime.execute_operator_command(
        command_id="opcmd_recover_reconciling_001",
        command_type="RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT",
        reason="unit stale runtime recovery",
        payload={"run_id": stuck.run_id},
    )
    assert operator_result.status == MiniQMTOperatorCommandStatus.EXECUTED
    recovered = scheduler.recover_no_side_effect_reconciling_run_after_operator_cleanup(
        run_id=stuck.run_id,
        operator_result=operator_result,
        source="unit_test",
    )
    assert recovered.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert recovered.run_payload_json["miniqmt_no_side_effect_reconciling_recovery"]["broker_called"] is False
    assert recovered.run_payload_json["submit_failure"]["stage"] == "MINIQMT_NO_SIDE_EFFECT_RECONCILING_RECOVERY"

    retried = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    latest = repo.get_simulation_daily_run(stuck.run_id)
    assert retried.results[0].status == "RECONCILED"
    assert latest.run_payload_json["broker_called"] is True
    assert len(broker.place_order_payloads) == len(plan.intents)


def test_scheduler_stale_runtime_recovery_rejects_non_reconciling_run() -> None:
    scheduler, repo, _broker, _qmt_binding = _miniqmt_shadow_test_scheduler()
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    run = planned.results[0].run
    assert run is not None
    repo.update_simulation_daily_run(
        run.run_id,
        status=SimulationDailyRunStatus.SUCCEEDED,
        payload_patch={"broker_called": False, "submitted_intents": 0},
    )

    with pytest.raises(RuntimeConfigInvalidError, match="MINIQMT_STALE_RUNTIME_RECOVERY_RUN_STATUS_UNSUPPORTED"):
        scheduler.require_no_side_effect_reconciling_run_for_operator_recovery(run_id=run.run_id)


@pytest.mark.parametrize(
    ("broker_called", "submitted_intents"),
    [
        (True, 0),
        (False, 1),
    ],
)
def test_scheduler_stale_runtime_recovery_rejects_run_with_side_effect_evidence(
    broker_called: bool,
    submitted_intents: int,
) -> None:
    scheduler, repo, _broker, _qmt_binding = _miniqmt_shadow_test_scheduler()
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    run = planned.results[0].run
    assert run is not None
    repo.update_simulation_daily_run(
        run.run_id,
        status=SimulationDailyRunStatus.RECONCILING,
        payload_patch={"broker_called": broker_called, "submitted_intents": submitted_intents},
    )

    with pytest.raises(RuntimeConfigInvalidError, match="MINIQMT_STALE_RUNTIME_RECOVERY_RUN_HAS_SIDE_EFFECT_EVIDENCE"):
        scheduler.require_no_side_effect_reconciling_run_for_operator_recovery(run_id=run.run_id)


@pytest.mark.parametrize(
    "operator_result",
    [
        SimpleNamespace(
            command_id="opcmd_recover_rejected",
            command_type="RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT",
            status=MiniQMTOperatorCommandStatus.REJECTED,
            metadata={"broker_evidence": {"broker_open_order_count": 0}, "broker_mutated": False},
        ),
        SimpleNamespace(
            command_id="opcmd_recover_open_broker",
            command_type="RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT",
            status=MiniQMTOperatorCommandStatus.EXECUTED,
            metadata={"broker_evidence": {"broker_open_order_count": 1}, "broker_mutated": False},
        ),
        SimpleNamespace(
            command_id="opcmd_recover_mutated_broker",
            command_type="RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT",
            status=MiniQMTOperatorCommandStatus.EXECUTED,
            metadata={"broker_evidence": {"broker_open_order_count": 0}, "broker_mutated": True},
        ),
    ],
)
def test_scheduler_stale_runtime_recovery_rejects_bad_operator_evidence(operator_result: SimpleNamespace) -> None:
    scheduler, repo, _broker, _qmt_binding = _miniqmt_shadow_test_scheduler()
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    run = planned.results[0].run
    assert run is not None
    repo.update_simulation_daily_run(
        run.run_id,
        status=SimulationDailyRunStatus.RECONCILING,
        payload_patch={"broker_called": False, "submitted_intents": 0},
    )

    with pytest.raises(RuntimeConfigInvalidError, match="MINIQMT_STALE_RUNTIME_RECOVERY_OPERATOR_EVIDENCE_REJECTED"):
        scheduler.recover_no_side_effect_reconciling_run_after_operator_cleanup(
            run_id=run.run_id,
            operator_result=operator_result,
            source="unit_test_bad_operator_evidence",
        )


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
    assert result.results[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
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
    assert result.results[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
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
