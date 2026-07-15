"""Scheduler entry point for unified LocalSim and MiniQMT simulation runs.

The scheduler is intentionally broker-neutral until ``submit=True`` is passed.
It drives StrategyRuntimeRelease -> DailySelectionEvidence -> ExecutionPlan for
eligible SimulationReleaseBinding rows and reuses persisted plans on restart so
that a backend tick cannot duplicate orders.
"""

from __future__ import annotations

import logging
import math
import os
import queue
import inspect
import threading
import time as monotonic_time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from copy import deepcopy
from dataclasses import asdict, dataclass, field, is_dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping, Protocol
import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.execution_algos.adaptive_is.reasons import QuoteContractError
from backend.services.paper_trading_v2.broker.base import BrokerBackend
from backend.services.paper_trading_v2.market_data import (
    MinuteDataSource,
    PreTradeTradabilityProvider,
    fetch_tdx_realtime_quotes,
)
from backend.services.paper_trading_v2.models import PaperRun
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.qmt_strategy_ledger.reconciliation import (
    QmtStrategyLedgerReconciliationService,
    broker_authoritative_strategy_projection,
)
from backend.services.qmt_strategy_ledger.order_service import SELL_ORDER_TYPE, OrderPreflightError, QmtManagedOrderService
from backend.services.qmt_strategy_ledger.models import (
    IntentPreflightStatus,
    IntentSubmitStatus,
    OrderBatchRecord,
    OrderBatchStatus,
    OrderIntentRecord,
    is_open_like_order_status,
    new_id as new_qmt_id,
)
from backend.services.qmt_strategy_ledger.sync_service import QmtStrategyLedgerSyncService
from backend.services.miniqmt_execution_runtime import (
    MiniQMTExecutionRuntimeKind,
)
from backend.services.selection_center.models import SelectionMode, SignalSnapshot
from backend.services.strategy_package.live_inference import AUTHORITATIVE_SELECTION_SCOPE, AUTHORITATIVE_SELECTION_SOURCE_TYPE
from backend.services.strategy_package.models import AlphaMode, PackageStatus, StrategyPackageManifest
from backend.services.strategy_package.multi_alpha_live import multi_alpha_selection_artifact_runtime_hash
from backend.services.strategy_package.runtime import _candidate_selection_artifact_runtime_hashes
from backend.services.strategy_package.selection_artifact import selection_artifact_runtime_hash
from backend.services.trading_calendar_status import TradingCalendarStatusService
from backend.services.trading_core.errors import (
    ArtifactGenerationFailedError,
    BrokerRejectedError,
    DataUnavailableError,
    RuntimeConfigInvalidError,
    SessionLockTimeoutError,
)
from backend.services.trading_core.models import AccountSnapshot, OrderSide, PositionLot, RunStatus

from .bridges import (
    LocalSimExecutionBridge,
    LocalSimExecutionSnapshot,
    LocalSimPlanSubmitResult,
    MiniQMTExecutionBridge,
)
from .lifecycle import (
    DEFAULT_SCHEDULER_WINDOWS,
    SCHEDULER_TZ,
    SCHEDULER_TZ_NAME,
    SimulationExecutionResult,
    SimulationLifecycleOrchestrator,
    SimulationPlanBuildResult,
    compute_schedule_windows,
    scheduler_now,
    scheduler_time,
)
from .models import (
    DailySelectionEvidence,
    ExecutionPlan,
    LocalSimExecutionRuntimeStatus,
    LocalSimExecutionStateV1,
    LocalSimMarketMarkProvenance,
    LocalSimMarketMarkV1,
    LocalSimProjectionOutboxStatus,
    LocalSimProjectionOutboxV1,
    LocalSimProjectionReceiptV1,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
    SimulationDailyRun,
    SimulationDailyRunStatus,
    SimulationReleaseBinding,
    StrategyRuntimeRelease,
    canonical_json_sha256,
)
from .miniqmt_quote_activation import build_miniqmt_quote_ingress_activation_from_env
from .repository import InMemorySimulationRuntimeRepository, SimulationRuntimeRepository
from .selection import StrategyPackageSelectionResult, StrategyPackageSelectionService
from .service import StrategyRuntimeReleaseService
from .tca_eod_observation import TcaEodObservationHook
from .tca_observation_metrics import TcaObservationMetricsEmitter
from .performance import (
    StrategyPerformanceProjectionService,
    with_miniqmt_capacity_residual_observability,
)
from .tail import TailHandlingPolicyService


# SIM scheduling is single-user and must not require an approval transition.
# RETIRED remains an explicit operational stop state, not an approval gate.
DEFAULT_SCHEDULER_SIM_BINDING_STATES = tuple(
    state
    for state in SimulationBindingApprovalState
    if state is not SimulationBindingApprovalState.RETIRED
)
MINIQMT_REALTIME_QUOTE_SOURCE = "MINIQMT_REALTIME.broker_quote"
SIMULATION_SELECTION_INFERENCE_TIMEOUT_ENV = "SIMULATION_RUNTIME_SELECTION_INFERENCE_TIMEOUT_SEC"
SIMULATION_SELECTION_INFERENCE_MAX_WORKERS_ENV = "SIMULATION_RUNTIME_SELECTION_INFERENCE_MAX_WORKERS"
SIMULATION_BINDING_WATCHDOG_TIMEOUT_ENV = "SIMULATION_RUNTIME_BINDING_WATCHDOG_TIMEOUT_SEC"
SIMULATION_MINIQMT_SUBMIT_TIMEOUT_ENV = "SIMULATION_RUNTIME_MINIQMT_SUBMIT_TIMEOUT_SEC"
SIMULATION_MINIQMT_RECONCILE_TIMEOUT_ENV = "SIMULATION_RUNTIME_MINIQMT_RECONCILE_TIMEOUT_SEC"
SIMULATION_MINIQMT_TICK_DRIVER_TIMEOUT_ENV = "SIMULATION_RUNTIME_MINIQMT_TICK_DRIVER_TIMEOUT_SEC"
DEFAULT_SIMULATION_BINDING_WATCHDOG_TIMEOUT_SECONDS = 600.0
DEFAULT_MINIQMT_SUBMIT_TIMEOUT_SECONDS = 120.0
DEFAULT_MINIQMT_RECONCILE_TIMEOUT_SECONDS = 120.0
DEFAULT_MINIQMT_TICK_DRIVER_TIMEOUT_SECONDS = 30.0
_MINIQMT_QUOTE_CONTEXT_PREPARE_FAILURE_STAGE = "MINIQMT_QUOTE_CONTEXT_PREPARE_FAILED"
_LEGACY_B0_CONTEXT_MISSING_FAILURE_STAGE = "MINIQMT_EVENT_LOOP_SUBMIT_FAILED"
_LEGACY_B0_CONTEXT_MISSING_MESSAGE = "B0_QUOTE_V2 controller requires scheduler-published context"

logger = logging.getLogger("aistock.simulation_runtime.scheduler")
_POST_CLOSE_RECONCILE_TIME = time(15, 0)


def _build_dynamic_target_equity_basis(
    *,
    binding: SimulationReleaseBinding,
    cash: float,
    frozen_cash: float,
    positions: dict[str, PositionLot],
    prices: dict[str, float],
    source: str,
) -> tuple[float, dict[str, Any]]:
    invalid_marks: list[dict[str, Any]] = []
    market_value = 0.0
    for symbol, position in positions.items():
        quantity = int(position.quantity)
        if quantity <= 0:
            continue
        raw_price = prices.get(symbol)
        try:
            price = float(raw_price) if raw_price is not None else float("nan")
        except (TypeError, ValueError):
            price = float("nan")
        if not math.isfinite(price) or price <= 0:
            invalid_marks.append({"symbol": symbol, "price": raw_price})
            continue
        market_value += quantity * price
    if invalid_marks:
        raise DataUnavailableError(
            "dynamic target sizing requires positive finite marks for all held positions",
            context={
                "strategy_id": binding.strategy_id,
                "binding_id": binding.binding_id,
                "broker_backend": binding.broker_backend.value,
                "invalid_marks": invalid_marks,
            },
        )
    total_equity = float(cash) + float(frozen_cash) + market_value
    if total_equity <= 0:
        raise RuntimeConfigInvalidError(
            "dynamic target sizing requires positive strategy-slot total_equity",
            context={
                "strategy_id": binding.strategy_id,
                "binding_id": binding.binding_id,
                "broker_backend": binding.broker_backend.value,
                "cash": float(cash),
                "frozen_cash": float(frozen_cash),
                "market_value": market_value,
            },
        )
    return total_equity, {
        "schema_version": "simulation_target_equity_basis_v1",
        "source": source,
        "strategy_id": binding.strategy_id,
        "binding_id": binding.binding_id,
        "broker_backend": binding.broker_backend.value,
        "cash": float(cash),
        "frozen_cash": float(frozen_cash),
        "market_value": market_value,
        "total_equity": total_equity,
        "capital_allocation": float(binding.capital_allocation),
        "position_count": sum(1 for position in positions.values() if int(position.quantity) > 0),
    }


_MINIQMT_DEPENDENT_BUY_RETRY_ERROR_CODES = frozenset(
    {
        "SELL_PROCEEDS_REQUIRED",
        "ACCOUNT_GROUP_SELL_PROCEEDS_REQUIRED",
    }
)
_MINIQMT_CAPACITY_RESIDUAL_RETRY_ERROR_CODES = frozenset(
    {
        "SKIPPED_INSUFFICIENT_CAPITAL",
    }
)
_MINIQMT_RETRYABLE_BUY_RESIDUAL_ERROR_CODES = (
    _MINIQMT_DEPENDENT_BUY_RETRY_ERROR_CODES | _MINIQMT_CAPACITY_RESIDUAL_RETRY_ERROR_CODES
)
_MINIQMT_STALE_ACTIVE_STATUSES = (
    SimulationDailyRunStatus.CREATED,
    SimulationDailyRunStatus.PRECHECKING,
    SimulationDailyRunStatus.SIGNAL_GENERATING,
    SimulationDailyRunStatus.TARGET_GENERATING,
    SimulationDailyRunStatus.PLANNING_EXECUTION,
    SimulationDailyRunStatus.SUBMITTING,
    SimulationDailyRunStatus.INTRADAY_RUNNING,
    SimulationDailyRunStatus.TAIL_HANDLING,
    SimulationDailyRunStatus.RECONCILING,
)
_LOCALSIM_STALE_ACTIVE_STATUSES = (
    SimulationDailyRunStatus.CREATED,
    SimulationDailyRunStatus.PRECHECKING,
    SimulationDailyRunStatus.SIGNAL_GENERATING,
    SimulationDailyRunStatus.TARGET_GENERATING,
    SimulationDailyRunStatus.PLANNING_EXECUTION,
    SimulationDailyRunStatus.SUBMITTING,
    SimulationDailyRunStatus.INTRADAY_RUNNING,
    SimulationDailyRunStatus.TAIL_HANDLING,
    SimulationDailyRunStatus.RECONCILING,
)

_LOCALSIM_ROLL_FORWARD_CREATED_BY = "simulation_lifecycle_scheduler.localsim_roll_forward"
_MINIQMT_ROLL_FORWARD_CREATED_BY = "simulation_lifecycle_scheduler.miniqmt_roll_forward"
_LOCALSIM_CASH_FIT_BUY_BUFFER_RATIO = 1.02
_LOCALSIM_CASH_FIT_SELL_PROCEEDS_BUFFER_RATIO = 0.98
_LOCALSIM_DEFAULT_OPEN_COST = 0.000095
_LOCALSIM_DEFAULT_CLOSE_COST = 0.000595
_LOCALSIM_DEFAULT_MIN_FEE = 5.0
_LOCALSIM_PROJECTION_MAX_ATTEMPTS = 3
_LOCALSIM_PROJECTION_RETRYABLE_PG_CODES = frozenset({"40001", "40P01", "55P03"})


@dataclass(frozen=True)
class SimulationRunContext:
    """Authoritative run context supplied by the target broker/account adapter."""

    current_positions: dict[str, PositionLot]
    current_prices: dict[str, float] = field(default_factory=dict)
    portfolio_id: str | None = None
    manifest: StrategyPackageManifest | None = None
    top_k: int | None = None
    execution_policy_payload: dict[str, Any] | None = None
    tail_policy_payload: dict[str, Any] | None = None
    local_broker: BrokerBackend | None = None
    managed_order_service: QmtManagedOrderService | None = None
    qmt_sync_service: QmtStrategyLedgerSyncService | None = None
    qmt_reconciliation_service: QmtStrategyLedgerReconciliationService | None = None
    qmt_ledger_repository: Any | None = None
    broker_positions: list[dict[str, Any]] = field(default_factory=list)
    context_diagnostics: dict[str, Any] = field(default_factory=dict)
    tail_policy_service: TailHandlingPolicyService | None = None
    price_by_symbol: dict[str, Any] | None = None
    paper_repository: Any | None = None
    cash: float | None = None
    frozen_cash: float = 0.0
    realized_pnl: float = 0.0
    market_data_source: str | None = None
    pre_trade_tradability: dict[str, dict[str, Any]] = field(default_factory=dict)
    target_total_equity: float | None = None
    target_equity_context: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LocalSimPersistenceResult:
    payload: dict[str, Any]
    positions: dict[str, PositionLot]
    marks: dict[str, float]
    cash: float
    economic_receipt_id: str
    outbox_id: str
    generation: int
    performance_payload: dict[str, Any]


class SimulationRunContextProvider(Protocol):
    def load_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> SimulationRunContext:
        """Return current positions/prices and optional broker services for one binding."""


class FailFastSimulationRunContextProvider:
    """Default provider that prevents silent empty-position or empty-price success."""

    provider_mode = "fail_fast"

    def load_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> SimulationRunContext:
        raise DataUnavailableError(
            "simulation lifecycle scheduler requires an explicit run context provider",
            context={
                "release_id": runtime_release.release_id,
                "binding_id": binding.binding_id,
                "strategy_id": binding.strategy_id,
                "trade_date": trade_date.isoformat(),
            },
        )

    def status(self) -> dict[str, Any]:
        return {
            "provider_mode": self.provider_mode,
            "provider_name": type(self).__name__,
            "ready": False,
            "diagnostic": (
                "Set SIMULATION_RUNTIME_CONTEXT_PROVIDER=production or "
                "ENABLE_SIMULATION_RUNTIME_PRODUCTION_PROVIDER=1 to enable the "
                "production SimulationRunContextProvider."
            ),
        }


class StaticSimulationRunContextProvider:
    """Explicit static provider for deterministic tests and controlled validation."""

    def __init__(
        self,
        *,
        by_binding_id: dict[str, SimulationRunContext] | None = None,
        by_strategy_id: dict[str, SimulationRunContext] | None = None,
    ) -> None:
        self._by_binding_id = dict(by_binding_id or {})
        self._by_strategy_id = dict(by_strategy_id or {})

    def load_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> SimulationRunContext:
        if binding.binding_id in self._by_binding_id:
            return self._by_binding_id[binding.binding_id]
        if binding.strategy_id in self._by_strategy_id:
            return self._by_strategy_id[binding.strategy_id]
        raise DataUnavailableError(
            "static simulation run context is missing binding context",
            context={
                "release_id": runtime_release.release_id,
                "binding_id": binding.binding_id,
                "strategy_id": binding.strategy_id,
                "trade_date": trade_date.isoformat(),
            },
        )


class ProductionSimulationRunContextProvider:
    """Production provider that loads positions, prices, and broker services from
    persisted state and live market data.

    The provider uses persisted Paper v2 portfolios for LocalSim state and the
    qmt_strategy virtual ledger for MiniQMT strategy state. Optional factories
    keep tests deterministic, but the default path is no longer an empty-state
    placeholder: missing repositories, accounts, positions, or prices fail fast
    with actionable diagnostics.
    """

    provider_mode = "production"

    def __init__(
        self,
        *,
        position_loader: Callable[[str, date], dict[str, PositionLot]] | None = None,
        price_loader: Callable[[list[str], date], dict[str, float]] | None = None,
        paper_repository_factory: Callable[[], Any] | None = None,
        qmt_repository_factory: Callable[[], Any] | None = None,
        qmt_client_factory: Callable[[], Any] | None = None,
        qmt_calendar_provider_factory: Callable[[], Any] | None = None,
        local_broker_factory: Callable[[str], BrokerBackend] | None = None,
        managed_order_service_factory: Callable[[], QmtManagedOrderService] | None = None,
        qmt_sync_service_factory: Callable[[], QmtStrategyLedgerSyncService] | None = None,
        qmt_reconciliation_service_factory: Callable[[], QmtStrategyLedgerReconciliationService] | None = None,
        qmt_ledger_repository: Any | None = None,
        package_manifest_loader: Callable[[str], StrategyPackageManifest | dict[str, Any] | None] | None = None,
        runtime_repository: SimulationRuntimeRepository | InMemorySimulationRuntimeRepository | Any | None = None,
        pre_trade_tradability_provider: PreTradeTradabilityProvider | Any | None = None,
        enable_localsim_broker: bool | None = None,
        enable_miniqmt_submit: bool | None = None,
    ) -> None:
        self._position_loader = position_loader
        self._price_loader = price_loader or _default_price_loader
        self._paper_repository_factory = paper_repository_factory or _default_paper_repository_factory
        self._qmt_repository_factory = qmt_repository_factory or _default_qmt_repository_factory
        self._qmt_client_factory = qmt_client_factory or _default_qmt_client_factory
        self._qmt_calendar_provider_factory = qmt_calendar_provider_factory or _default_qmt_calendar_provider
        self._local_broker_factory = local_broker_factory
        self._managed_order_service_factory = managed_order_service_factory
        self._qmt_sync_service_factory = qmt_sync_service_factory
        self._qmt_reconciliation_service_factory = qmt_reconciliation_service_factory
        self._qmt_ledger_repository = qmt_ledger_repository
        self._package_manifest_loader = package_manifest_loader or _default_strategy_package_manifest_loader
        self._runtime_repository = runtime_repository
        self._pre_trade_tradability_provider_injected = pre_trade_tradability_provider is not None
        self._pre_trade_tradability_provider = pre_trade_tradability_provider or PreTradeTradabilityProvider(
            realtime_quote_fetcher=fetch_tdx_realtime_quotes,
            realtime_quote_source="TDX_REALTIME.batch_quote",
        )
        self._enable_localsim_broker = (
            _env_flag("SIMULATION_RUNTIME_ENABLE_LOCALSIM_BROKER", default=True)
            if enable_localsim_broker is None
            else bool(enable_localsim_broker)
        )
        self._enable_miniqmt_submit = (
            (
                _env_flag("AISTOCK_ALLOW_MINIQMT_MANAGED_ORDERS", default=False)
                or _env_flag("AISTOCK_ALLOW_MINIQMT_SUBMIT_TEST", default=False)
            )
            if enable_miniqmt_submit is None
            else bool(enable_miniqmt_submit)
        )

    def status(self) -> dict[str, Any]:
        return {
            "provider_mode": self.provider_mode,
            "provider_name": type(self).__name__,
            "ready": True,
            "localsim_state_source": "paper_v2_portfolio",
            "miniqmt_state_source": "broker_authoritative_positions_with_strategy_slot_projection",
            "market_price_source": "market.kline_daily_raw_latest_close",
            "pre_trade_tradability_gate": {
                "source": type(self._pre_trade_tradability_provider).__name__,
                "localsim_same_day_quote_required": True,
                "miniqmt_quote_required": True,
                "miniqmt_quote_source": MINIQMT_REALTIME_QUOTE_SOURCE,
                "historical_quote_required": False,
            },
            "localsim_market_data_source_policy": {
                "same_day": MinuteDataSource.TDX_REALTIME.value,
                "historical": "persisted_portfolio_data_source",
            },
            "localsim_broker_enabled": self._enable_localsim_broker or self._local_broker_factory is not None,
            "miniqmt_preview_enabled": True,
            "miniqmt_submit_enabled": self._enable_miniqmt_submit,
            "miniqmt_submit_default": False,
        }

    def load_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> SimulationRunContext:
        return self.load_context_for_phase(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
            as_of_time=None,
            require_localsim_realtime_quote=(trade_date == date.today()),
        )

    def load_context_for_phase(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
        as_of_time: datetime | None,
        require_localsim_realtime_quote: bool,
    ) -> SimulationRunContext:
        if binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM:
            return self._load_local_sim_context(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
                as_of_time=as_of_time,
                require_realtime_quote=require_localsim_realtime_quote,
            )

        if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM:
            return self._load_miniqmt_context(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
            )

        raise DataUnavailableError(
            "ProductionSimulationRunContextProvider: unsupported broker backend",
            context={
                "broker_backend": binding.broker_backend.value,
                "strategy_id": binding.strategy_id,
            },
        )

    def _load_local_sim_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
        as_of_time: datetime | None = None,
        require_realtime_quote: bool | None = None,
    ) -> SimulationRunContext:
        portfolio_id = self._resolve_local_sim_portfolio_id(binding)
        paper_repository = self._build_dependency(
            self._paper_repository_factory,
            "PaperTradingV2Repository",
            binding=binding,
            trade_date=trade_date,
        )
        try:
            portfolio = paper_repository.get_portfolio(portfolio_id)
            positions = (
                self._load_positions_with_injected_loader(binding.strategy_id, trade_date)
                if self._position_loader is not None
                else paper_repository.load_latest_positions(portfolio_id, trade_date)
            )
            positions, settlement_diagnostics = self._settle_local_sim_positions_for_trade_date(
                positions=positions,
                trade_date=trade_date,
                strategy_id=binding.strategy_id,
                binding_id=binding.binding_id,
            )
            cash = float(paper_repository.load_latest_cash(portfolio, trade_date))
        except (DataUnavailableError, RuntimeConfigInvalidError):
            raise
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "failed to load LocalSim production context from Paper v2 persisted state",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "portfolio_id": portfolio_id,
                    "trade_date": trade_date.isoformat(),
                },
            ) from exc
        prices = self._load_prices_for_positions(
            positions,
            trade_date,
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
        )
        market_data_source = self._resolve_local_sim_market_data_source(
            portfolio=portfolio,
            trade_date=trade_date,
            as_of_time=as_of_time,
        )
        pre_trade_tradability = self._load_pre_trade_tradability(
            symbols=list(positions),
            trade_date=trade_date,
            require_realtime_quote=bool(
                require_realtime_quote
                and market_data_source == MinuteDataSource.TDX_REALTIME
                and self._position_loader is None
            ),
            as_of_time=as_of_time,
        )
        manifest, manifest_identity_diagnostics = self._resolve_local_sim_manifest(
            portfolio_manifest=getattr(portfolio, "frozen_manifest", None),
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
        )
        release_execution_policy_payload = self._release_execution_policy_payload(runtime_release)
        effective_execution_policy_payload = self._resolve_local_sim_execution_policy(
            runtime_release=runtime_release,
            binding=binding,
            portfolio=portfolio,
        )
        broker_execution_policy = (
            effective_execution_policy_payload
            if self._local_broker_factory is None and self._enable_localsim_broker
            else None
        )
        local_broker = self._build_local_sim_broker(
            portfolio_id=portfolio_id,
            portfolio=portfolio,
            binding=binding,
            runtime_release=runtime_release,
            manifest=manifest,
            execution_policy=broker_execution_policy,
            cash=cash,
            positions=positions,
            trade_date=trade_date,
            as_of_time=as_of_time,
        )
        target_total_equity, target_equity_context = _build_dynamic_target_equity_basis(
            binding=binding,
            cash=cash,
            frozen_cash=0.0,
            positions=positions,
            prices=prices,
            source="paper_v2_portfolio_dynamic_equity",
        )
        return SimulationRunContext(
            current_positions=positions,
            current_prices=prices,
            portfolio_id=portfolio_id,
            manifest=manifest,
            execution_policy_payload=effective_execution_policy_payload or release_execution_policy_payload,
            local_broker=local_broker,
            paper_repository=paper_repository,
            cash=cash,
            target_total_equity=target_total_equity,
            target_equity_context=target_equity_context,
            context_diagnostics={
                "manifest_identity": manifest_identity_diagnostics,
                "localsim_tplus1_settlement": settlement_diagnostics,
                "pre_trade_tradability": self._pre_trade_tradability_diagnostics(pre_trade_tradability),
                "target_equity_basis": target_equity_context,
            },
            market_data_source=(
                getattr(local_broker, "data_source").value
                if local_broker is not None and getattr(local_broker, "data_source", None) is not None
                else market_data_source.value
            ),
            pre_trade_tradability=pre_trade_tradability,
        )

    @staticmethod
    def _settle_local_sim_positions_for_trade_date(
        *,
        positions: dict[str, PositionLot],
        trade_date: date,
        strategy_id: str,
        binding_id: str,
    ) -> tuple[dict[str, PositionLot], dict[str, Any]]:
        settled: dict[str, PositionLot] = {}
        adjusted: list[dict[str, Any]] = []
        for symbol, position in positions.items():
            updated = position
            if position.trade_date < trade_date and int(position.available_quantity) < int(position.quantity):
                updated = position.model_copy(update={"available_quantity": int(position.quantity)})
                adjusted.append(
                    {
                        "symbol": symbol,
                        "trade_date": position.trade_date.isoformat(),
                        "previous_available_quantity": int(position.available_quantity),
                        "settled_available_quantity": int(updated.available_quantity),
                        "quantity": int(position.quantity),
                    }
                )
            settled[symbol] = updated
        return settled, {
            "schema_version": "localsim_tplus1_settlement_v1",
            "strategy_id": strategy_id,
            "binding_id": binding_id,
            "trade_date": trade_date.isoformat(),
            "position_count": len(positions),
            "settled_position_count": len(adjusted),
            "settled_positions": adjusted,
        }

    def _load_miniqmt_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> SimulationRunContext:
        qmt_repository = self._qmt_ledger_repository or self._build_dependency(
            self._qmt_repository_factory,
            "QmtStrategyLedgerRepository",
            binding=binding,
            trade_date=trade_date,
        )
        need_qmt_client = (
            self._position_loader is None
            or self._managed_order_service_factory is None
            or self._qmt_sync_service_factory is None
        )
        qmt_client = self._qmt_client_factory() if need_qmt_client else None
        try:
            account = qmt_repository.get_virtual_account(binding.strategy_id)
            if self._position_loader is not None:
                broker_positions: list[dict[str, Any]] = []
                reconciliation_diagnostics: dict[str, Any] = {
                    "schema_version": "miniqmt_broker_position_reconciliation_v1",
                    "status": "skipped_injected_position_loader",
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "trade_date": trade_date.isoformat(),
                }
                positions = self._load_positions_with_injected_loader(binding.strategy_id, trade_date)
            else:
                broker_positions = self._load_miniqmt_broker_positions(
                    qmt_client,
                    binding=binding,
                    trade_date=trade_date,
                )
                ledger_positions = self._positions_from_qmt_lots(
                    repository=qmt_repository,
                    strategy_id=binding.strategy_id,
                )
                account_strategy_quantities = self._miniqmt_account_strategy_lot_quantities(
                    repository=qmt_repository,
                    account_id=account.account_id,
                )
                positions, reconciliation_diagnostics = self._reconcile_miniqmt_positions_with_broker(
                    ledger_positions,
                    broker_positions,
                    strategy_id=binding.strategy_id,
                    binding_id=binding.binding_id,
                    trade_date=trade_date,
                    account_strategy_quantities=account_strategy_quantities,
                )
        except (DataUnavailableError, RuntimeConfigInvalidError):
            raise
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "failed to load MiniQMT production context from qmt_strategy virtual ledger",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "broker_account_id": binding.broker_account_id,
                    "trade_date": trade_date.isoformat(),
                },
            ) from exc
        prices = self._load_prices_for_positions(
            positions,
            trade_date,
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
        )
        pre_trade_tradability = self._load_miniqmt_pre_trade_tradability(
            symbols=list(positions),
            trade_date=trade_date,
            binding=binding,
            qmt_client=qmt_client,
            require_realtime_quote=self._position_loader is None and trade_date == date.today(),
        )
        manifest = self._load_strategy_package_manifest(
            runtime_release=runtime_release,
            binding=binding,
        )
        managed_order_service = (
            self._managed_order_service_factory()
            if self._managed_order_service_factory is not None
            else self._build_managed_order_service(qmt_repository, broker=qmt_client)
        )
        if not self._enable_miniqmt_submit and self._managed_order_service_factory is None:
            managed_order_service = PreviewOnlyMiniQMTManagedOrderService(managed_order_service)
        qmt_sync_service = (
            self._qmt_sync_service_factory()
            if self._qmt_sync_service_factory is not None
            else QmtStrategyLedgerSyncService(
                repository=qmt_repository,
                qmt_client=qmt_client,
                account_id=binding.broker_account_id or account.account_id,
                trade_date=trade_date,
                calendar_provider=self._qmt_calendar_provider_factory(),
            )
        )
        qmt_reconciliation_service = (
            self._qmt_reconciliation_service_factory()
            if self._qmt_reconciliation_service_factory is not None
            else QmtStrategyLedgerReconciliationService(repository=qmt_repository)
        )
        cash = float(account.cash)
        frozen_cash = float(account.frozen_cash)
        target_total_equity, target_equity_context = _build_dynamic_target_equity_basis(
            binding=binding,
            cash=cash,
            frozen_cash=frozen_cash,
            positions=positions,
            prices=prices,
            source="miniqmt_strategy_slot_dynamic_equity",
        )
        return SimulationRunContext(
            current_positions=positions,
            current_prices=prices,
            portfolio_id=binding.strategy_id,
            manifest=manifest,
            managed_order_service=managed_order_service,
            qmt_sync_service=qmt_sync_service,
            qmt_reconciliation_service=qmt_reconciliation_service,
            qmt_ledger_repository=qmt_repository,
            broker_positions=broker_positions,
            context_diagnostics={
                "miniqmt_broker_position_reconciliation": reconciliation_diagnostics,
                "pre_trade_tradability": self._pre_trade_tradability_diagnostics(pre_trade_tradability),
                "target_equity_basis": target_equity_context,
            },
            cash=cash,
            frozen_cash=frozen_cash,
            realized_pnl=float(account.realized_pnl),
            target_total_equity=target_total_equity,
            target_equity_context=target_equity_context,
            price_by_symbol=prices,
            market_data_source=MinuteDataSource.MINIQMT_REALTIME.value,
            pre_trade_tradability=pre_trade_tradability,
        )

    def load_pre_trade_tradability(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        binding: SimulationReleaseBinding,
        market_data_source: str | None = None,
        require_realtime_quote: bool | None = None,
        as_of_time: datetime | None = None,
        side_by_symbol: dict[str, Any] | None = None,
    ) -> dict[str, dict[str, Any]]:
        if require_realtime_quote is None:
            current_trade_date = (
                date.today()
                if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
                else scheduler_time(as_of_time).date() if as_of_time is not None else date.today()
            )
            require_quote = self._position_loader is None and (
                (binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM and trade_date == current_trade_date)
                or (
                    binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM
                    and (
                        market_data_source == MinuteDataSource.TDX_REALTIME.value
                        or trade_date == current_trade_date
                    )
                )
            )
        else:
            require_quote = bool(require_realtime_quote and self._position_loader is None)
        if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM:
            qmt_client = self._qmt_client_factory() if require_quote else None
            return self._load_miniqmt_pre_trade_tradability(
                symbols=symbols,
                trade_date=trade_date,
                binding=binding,
                qmt_client=qmt_client,
                require_realtime_quote=require_quote,
                as_of_time=as_of_time,
                side_by_symbol=side_by_symbol,
            )
        return self._load_pre_trade_tradability(
            symbols=symbols,
            trade_date=trade_date,
            require_realtime_quote=require_quote,
            as_of_time=as_of_time,
            side_by_symbol=side_by_symbol,
        )

    def _load_miniqmt_pre_trade_tradability(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        binding: SimulationReleaseBinding,
        qmt_client: Any | None,
        require_realtime_quote: bool,
        as_of_time: datetime | None = None,
        side_by_symbol: dict[str, Any] | None = None,
    ) -> dict[str, dict[str, Any]]:
        if not require_realtime_quote:
            return {}
        quote_fetcher = self._build_miniqmt_quote_fetcher(
            qmt_client=qmt_client,
            binding=binding,
            trade_date=trade_date,
        )
        provider_kwargs: dict[str, Any] = {
            "realtime_quote_fetcher": quote_fetcher,
            "realtime_quote_source": MINIQMT_REALTIME_QUOTE_SOURCE,
        }
        injected_provider = self._pre_trade_tradability_provider if self._pre_trade_tradability_provider_injected else None
        suspend_status_provider = getattr(injected_provider, "suspend_status_provider", None)
        st_status_provider = getattr(injected_provider, "st_status_provider", None)
        if suspend_status_provider is not None:
            provider_kwargs["suspend_status_provider"] = suspend_status_provider
        if st_status_provider is not None:
            provider_kwargs["st_status_provider"] = st_status_provider
        provider = PreTradeTradabilityProvider(**provider_kwargs)
        return self._load_pre_trade_tradability(
            symbols=symbols,
            trade_date=trade_date,
            require_realtime_quote=True,
            provider=provider,
            as_of_time=as_of_time,
            side_by_symbol=side_by_symbol,
        )

    def _build_miniqmt_quote_fetcher(
        self,
        *,
        qmt_client: Any | None,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> Callable[[list[str]], dict[str, dict[str, Any]]]:
        if qmt_client is None:
            raise DataUnavailableError(
                "MiniQMT realtime quote fetch requires a MiniQMT broker client",
                context={
                    "reason_code": "MINIQMT_REALTIME_QUOTE_CLIENT_MISSING",
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "broker_account_id": binding.broker_account_id,
                    "trade_date": trade_date.isoformat(),
                    "quote_source": MINIQMT_REALTIME_QUOTE_SOURCE,
                },
            )

        def fetch(symbols: list[str]) -> dict[str, dict[str, Any]]:
            normalized_symbols = [str(symbol).strip() for symbol in symbols if str(symbol).strip()]
            if not normalized_symbols:
                return {}
            query_quote = getattr(qmt_client, "query_quote", None)
            try:
                if callable(query_quote):
                    quotes: dict[str, dict[str, Any]] = {}
                    for symbol in normalized_symbols:
                        row = query_quote(symbol)
                        if row is None:
                            continue
                        if not isinstance(row, dict):
                            raise DataUnavailableError(
                                "MiniQMT query_quote returned invalid quote payload",
                                context={
                                    "reason_code": "MINIQMT_REALTIME_QUOTE_PAYLOAD_INVALID",
                                    "strategy_id": binding.strategy_id,
                                    "binding_id": binding.binding_id,
                                    "trade_date": trade_date.isoformat(),
                                    "symbol": symbol,
                                    "quote_source": MINIQMT_REALTIME_QUOTE_SOURCE,
                                    "payload_type": type(row).__name__,
                                },
                            )
                        quotes[symbol] = dict(row)
                    return quotes

                get_full_tick = getattr(qmt_client, "get_full_tick", None)
                if not callable(get_full_tick):
                    raise DataUnavailableError(
                        "MiniQMT realtime quote client must expose get_full_tick or query_quote",
                        context={
                            "reason_code": "MINIQMT_REALTIME_QUOTE_FETCHER_MISSING",
                            "strategy_id": binding.strategy_id,
                            "binding_id": binding.binding_id,
                            "broker_account_id": binding.broker_account_id,
                            "trade_date": trade_date.isoformat(),
                            "quote_source": MINIQMT_REALTIME_QUOTE_SOURCE,
                            "client_type": type(qmt_client).__name__,
                        },
                    )
                raw_payload = get_full_tick(normalized_symbols)
                if not isinstance(raw_payload, dict):
                    raise DataUnavailableError(
                        "MiniQMT get_full_tick returned invalid quote payload",
                        context={
                            "reason_code": "MINIQMT_REALTIME_QUOTE_PAYLOAD_INVALID",
                            "strategy_id": binding.strategy_id,
                            "binding_id": binding.binding_id,
                            "trade_date": trade_date.isoformat(),
                            "quote_source": MINIQMT_REALTIME_QUOTE_SOURCE,
                            "payload_type": type(raw_payload).__name__,
                        },
                    )
                from backend.services.paper_trading_v2.broker.minqmtsim import normalize_miniqmt_quote_row

                quotes: dict[str, dict[str, Any]] = {}
                for symbol in normalized_symbols:
                    row = raw_payload.get(symbol)
                    if row is None:
                        raw_code = symbol.split(".")[0]
                        for key, value in raw_payload.items():
                            if str(key).split(".")[0] == raw_code:
                                row = value
                                break
                    if row is None:
                        continue
                    if not isinstance(row, dict):
                        raise DataUnavailableError(
                            "MiniQMT get_full_tick returned invalid quote row",
                            context={
                                "reason_code": "MINIQMT_REALTIME_QUOTE_ROW_INVALID",
                                "strategy_id": binding.strategy_id,
                                "binding_id": binding.binding_id,
                                "trade_date": trade_date.isoformat(),
                                "symbol": symbol,
                                "quote_source": MINIQMT_REALTIME_QUOTE_SOURCE,
                                "payload_type": type(row).__name__,
                            },
                        )
                    quotes[symbol] = dict(normalize_miniqmt_quote_row(symbol, row))
                return quotes
            except DataUnavailableError:
                raise
            except Exception as exc:  # noqa: BLE001
                raise DataUnavailableError(
                    "MiniQMT realtime quote fetch failed",
                    context={
                        "reason_code": "MINIQMT_REALTIME_QUOTE_FETCH_FAILED",
                        "strategy_id": binding.strategy_id,
                        "binding_id": binding.binding_id,
                        "broker_account_id": binding.broker_account_id,
                        "trade_date": trade_date.isoformat(),
                        "symbols": normalized_symbols,
                        "quote_source": MINIQMT_REALTIME_QUOTE_SOURCE,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    },
                ) from exc

        return fetch

    def _load_pre_trade_tradability(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        require_realtime_quote: bool,
        provider: Any | None = None,
        as_of_time: datetime | None = None,
        side_by_symbol: dict[str, Any] | None = None,
    ) -> dict[str, dict[str, Any]]:
        if self._position_loader is not None and not self._pre_trade_tradability_provider_injected:
            return {}
        tradability_provider = provider or self._pre_trade_tradability_provider
        loader = getattr(tradability_provider, "get_statuses", None)
        if not callable(loader):
            raise DataUnavailableError(
                "pre-trade tradability provider must expose get_statuses",
                context={
                    "reason_code": "PRE_TRADE_TRADABILITY_PROVIDER_METHOD_MISSING",
                    "provider": type(tradability_provider).__name__,
                },
            )
        signature = inspect.signature(loader)
        accepts_kwargs = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )
        optional_kwargs = {
            "require_realtime_quote": require_realtime_quote,
            "as_of_time": as_of_time,
            "side_by_symbol": side_by_symbol,
        }
        supported_kwargs = {
            key: value
            for key, value in optional_kwargs.items()
            if accepts_kwargs or key in signature.parameters
        }
        raw = loader(symbols, trade_date, **supported_kwargs)
        if not isinstance(raw, dict):
            raise DataUnavailableError(
                "pre-trade tradability provider returned invalid payload",
                context={
                    "reason_code": "PRE_TRADE_TRADABILITY_PROVIDER_INVALID_PAYLOAD",
                    "provider": type(tradability_provider).__name__,
                    "payload_type": type(raw).__name__,
                },
            )
        return {str(symbol): dict(status) for symbol, status in raw.items() if isinstance(status, dict)}

    @staticmethod
    def _pre_trade_tradability_diagnostics(statuses: dict[str, dict[str, Any]]) -> dict[str, Any]:
        blocked = [
            {
                "symbol": symbol,
                "reason_code": status.get("reason_code"),
                "source": status.get("source"),
            }
            for symbol, status in sorted(statuses.items())
            if isinstance(status, dict) and not bool(status.get("is_tradable", True))
        ]
        return {
            "schema_version": "pre_trade_tradability_diagnostics_v1",
            "symbol_count": len(statuses),
            "blocked_symbol_count": len(blocked),
            "blocked_symbols": blocked,
        }

    def _build_managed_order_service(self, qmt_repository: Any, *, broker: Any | None = None) -> QmtManagedOrderService:
        broker = broker if broker is not None else self._qmt_client_factory()
        return QmtManagedOrderService(
            repository=qmt_repository,
            broker=broker,
            calendar_provider=self._qmt_calendar_provider_factory(),
        )

    def _build_local_sim_broker(
        self,
        *,
        portfolio_id: str,
        portfolio: Any,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        manifest: StrategyPackageManifest | None,
        execution_policy: dict[str, Any] | None,
        cash: float,
        positions: dict[str, PositionLot],
        trade_date: date,
        as_of_time: datetime | None = None,
    ) -> BrokerBackend | None:
        if self._local_broker_factory is not None:
            return self._local_broker_factory(binding.strategy_id)
        if not self._enable_localsim_broker:
            return None
        if manifest is None:
            raise DataUnavailableError(
                "LocalSim production context requires a frozen StrategyPackage manifest",
                context={"portfolio_id": portfolio_id, "binding_id": binding.binding_id},
            )
        try:
            from backend.services.paper_trading_v2.broker.localsim import LocalSimBackend

            data_source = self._resolve_local_sim_market_data_source(
                portfolio=portfolio,
                trade_date=trade_date,
                as_of_time=as_of_time,
            )
            return LocalSimBackend(
                portfolio_id=portfolio_id,
                initial_cash=float(getattr(portfolio, "initial_cash", binding.capital_allocation)),
                initial_available_cash=cash,
                initial_positions=positions,
                data_source=data_source,
                manifest=manifest,
                execution_policy=execution_policy,
                package_id=binding.package_id,
                scheduler_as_of_time=as_of_time,
            )
        except (DataUnavailableError, RuntimeConfigInvalidError):
            raise
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "failed to construct LocalSim production broker from persisted context",
                context={
                    "portfolio_id": portfolio_id,
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "execution_policy_version_id": runtime_release.execution_policy_version_id,
                    "execution_policy_sha256": runtime_release.execution_policy_sha256,
                },
            ) from exc

    @staticmethod
    def _resolve_local_sim_market_data_source(
        *,
        portfolio: Any,
        trade_date: date,
        as_of_time: datetime | None = None,
    ) -> MinuteDataSource:
        data_source = getattr(portfolio, "data_source", MinuteDataSource.DB_HISTORICAL)
        if not isinstance(data_source, MinuteDataSource):
            data_source = MinuteDataSource(str(data_source))
        current_trade_date = scheduler_time(as_of_time).date() if as_of_time is not None else date.today()
        if trade_date == current_trade_date:
            return MinuteDataSource.TDX_REALTIME
        return data_source

    @staticmethod
    def _release_execution_policy_payload(runtime_release: StrategyRuntimeRelease) -> dict[str, Any] | None:
        release_config = runtime_release.release_config_json if isinstance(runtime_release.release_config_json, dict) else {}
        payload = release_config.get("execution_policy")
        if not isinstance(payload, dict):
            return None
        return dict(payload)

    def _resolve_local_sim_execution_policy(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        portfolio: Any,
    ) -> dict[str, Any] | None:
        release_policy = self._release_execution_policy_payload(runtime_release)
        if self._has_execution_policy_snapshot(release_policy):
            return release_policy

        portfolio_policy = getattr(portfolio, "execution_policy", None)
        if not isinstance(portfolio_policy, dict) or not portfolio_policy:
            return None

        if not isinstance(release_policy, dict) or not release_policy:
            return dict(portfolio_policy)

        release_policy_id = self._policy_id_from_payload(
            release_policy,
            fallback=runtime_release.execution_policy_version_id,
        )
        release_policy_sha = self._policy_sha_from_payload(
            release_policy,
            fallback=runtime_release.execution_policy_sha256,
        )
        portfolio_policy_id = self._policy_id_from_payload(portfolio_policy)
        portfolio_policy_sha = self._policy_sha_from_payload(portfolio_policy)
        if (
            self._policy_ids_differ(release_policy_id, portfolio_policy_id)
            or self._policy_shas_differ(release_policy_sha, portfolio_policy_sha)
            or self._policy_id_indicates_vnpy_style(release_policy_id)
        ):
            portfolio_json = portfolio_policy.get("policy_json") if isinstance(portfolio_policy.get("policy_json"), dict) else {}
            raise RuntimeConfigInvalidError(
                "LocalSim runtime release execution policy snapshot is missing full policy_json",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "portfolio_id": getattr(portfolio, "portfolio_id", None),
                    "release_execution_policy_version_id": runtime_release.execution_policy_version_id,
                    "release_execution_policy_sha256": runtime_release.execution_policy_sha256,
                    "release_policy_payload": release_policy,
                    "portfolio_policy_id": portfolio_policy_id,
                    "portfolio_policy_sha256": portfolio_policy_sha,
                    "portfolio_policy_algo_code": portfolio_json.get("algo_code"),
                    "required_action": (
                        "store a full LocalSim-compatible execution policy snapshot in the runtime release "
                        "or bind the vn.py-style release only to MiniQMT"
                    ),
                },
            )
        return dict(portfolio_policy)

    @staticmethod
    def _has_execution_policy_snapshot(payload: dict[str, Any] | None) -> bool:
        if not isinstance(payload, dict):
            return False
        return isinstance(payload.get("policy_json"), dict) or bool(str(payload.get("algo_code") or "").strip())

    @staticmethod
    def _policy_id_from_payload(payload: dict[str, Any], *, fallback: str | None = None) -> str | None:
        for key in ("validated_execution_policy_id", "policy_id", "policy_version_id"):
            value = str(payload.get(key) or "").strip()
            if value:
                return value
        value = str(fallback or "").strip()
        return value or None

    @staticmethod
    def _policy_sha_from_payload(payload: dict[str, Any], *, fallback: str | None = None) -> str | None:
        for key in ("policy_sha256", "sha256"):
            value = str(payload.get(key) or "").strip()
            if value:
                return value
        value = str(fallback or "").strip()
        return value or None

    @staticmethod
    def _policy_ids_differ(left: str | None, right: str | None) -> bool:
        return bool(left and right and left != right)

    @staticmethod
    def _policy_shas_differ(left: str | None, right: str | None) -> bool:
        return bool(left and right and left != right)

    @staticmethod
    def _policy_id_indicates_vnpy_style(policy_id: str | None) -> bool:
        text = str(policy_id or "").strip().upper()
        return any(algo_code in text for algo_code in ("SNIPER_MINIQMT", "BEST_LIMIT_MINIQMT", "TWAP_LITE_MINIQMT"))

    def _load_positions_with_injected_loader(self, strategy_id: str, trade_date: date) -> dict[str, PositionLot]:
        if self._position_loader is None:
            raise DataUnavailableError(
                "production context provider has no injected position_loader",
                context={"strategy_id": strategy_id, "trade_date": trade_date.isoformat()},
            )
        try:
            return dict(self._position_loader(strategy_id, trade_date))
        except DataUnavailableError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "injected production position_loader failed",
                context={"strategy_id": strategy_id, "trade_date": trade_date.isoformat()},
            ) from exc

    def _load_prices_for_positions(
        self,
        positions: dict[str, PositionLot],
        trade_date: date,
        *,
        strategy_id: str,
        binding_id: str,
    ) -> dict[str, float]:
        symbols = sorted(positions)
        try:
            prices = dict(self._price_loader(symbols, trade_date))
        except DataUnavailableError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "production price_loader failed",
                context={
                    "strategy_id": strategy_id,
                    "binding_id": binding_id,
                    "symbols": symbols,
                    "trade_date": trade_date.isoformat(),
                },
            ) from exc
        missing = sorted(symbol for symbol in symbols if symbol not in prices)
        if missing:
            raise DataUnavailableError(
                "production price_loader did not return marks for all held positions",
                context={
                    "strategy_id": strategy_id,
                    "binding_id": binding_id,
                    "missing_symbols": missing,
                    "trade_date": trade_date.isoformat(),
                },
            )
        return prices

    @staticmethod
    def _positions_from_qmt_lots(*, repository: Any, strategy_id: str) -> dict[str, PositionLot]:
        lots = repository.list_position_lots(strategy_id)
        positions: dict[str, PositionLot] = {}
        for lot in lots:
            remaining = int(getattr(lot, "remaining_quantity", getattr(lot, "quantity", 0)))
            if remaining <= 0:
                continue
            symbol = str(lot.symbol)
            existing = positions.get(symbol)
            quantity = remaining + (existing.quantity if existing else 0)
            available_quantity = int(getattr(lot, "available_quantity", 0)) + (
                existing.available_quantity if existing else 0
            )
            cost_amount = Decimal(str(getattr(lot, "avg_cost"))) * Decimal(str(remaining))
            if existing is not None:
                cost_amount += Decimal(str(existing.avg_cost)) * Decimal(str(existing.quantity))
            avg_cost = float(cost_amount / Decimal(str(quantity))) if quantity else 0.0
            positions[symbol] = PositionLot(
                portfolio_id=strategy_id,
                symbol=symbol,
                quantity=quantity,
                available_quantity=available_quantity,
                avg_cost=avg_cost,
                trade_date=getattr(lot, "open_date"),
            )
        return positions

    @staticmethod
    def _miniqmt_account_strategy_lot_quantities(*, repository: Any, account_id: str) -> dict[str, dict[str, int]]:
        quantities: dict[str, dict[str, int]] = {}
        for account in repository.list_virtual_accounts(account_id=account_id):
            by_symbol: dict[str, int] = {}
            for lot in repository.list_position_lots(account.strategy_id):
                remaining = int(getattr(lot, "remaining_quantity", getattr(lot, "quantity", 0)) or 0)
                if remaining <= 0:
                    continue
                symbol = str(lot.symbol)
                by_symbol[symbol] = by_symbol.get(symbol, 0) + remaining
            quantities[account.strategy_id] = dict(sorted(by_symbol.items()))
        return quantities

    def _load_miniqmt_broker_positions(
        self,
        qmt_client: Any,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> list[dict[str, Any]]:
        get_positions = getattr(qmt_client, "get_positions", None)
        if not callable(get_positions):
            raise DataUnavailableError(
                "MiniQMT production context requires broker get_positions",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "broker_account_id": binding.broker_account_id,
                    "trade_date": trade_date.isoformat(),
                },
            )
        try:
            return [dict(position) for position in get_positions()]
        except DataUnavailableError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "failed to load MiniQMT broker positions for strategy-lot reconciliation",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "broker_account_id": binding.broker_account_id,
                    "trade_date": trade_date.isoformat(),
                },
            ) from exc

    @staticmethod
    def _reconcile_miniqmt_positions_with_broker(
        ledger_positions: dict[str, PositionLot],
        broker_positions: list[dict[str, Any]],
        *,
        strategy_id: str,
        binding_id: str,
        trade_date: date,
        account_strategy_quantities: dict[str, dict[str, int]] | None = None,
    ) -> tuple[dict[str, PositionLot], dict[str, Any]]:
        broker_totals = _broker_position_totals(broker_positions)
        broker_quantities = {symbol: quantity for symbol, (quantity, _can_sell) in broker_totals.items()}
        strategy_quantities = account_strategy_quantities or {
            strategy_id: {symbol: int(position.quantity) for symbol, position in ledger_positions.items()}
        }
        projection = broker_authoritative_strategy_projection(
            strategy_lot_quantities=strategy_quantities,
            broker_quantities=broker_quantities,
        )
        projected_for_strategy = projection.projected_quantities.get(strategy_id, {})
        reconciled: dict[str, PositionLot] = {}
        dropped: list[dict[str, Any]] = []
        capped: list[dict[str, Any]] = []
        for symbol, position in sorted(ledger_positions.items()):
            broker_quantity, broker_can_sell = broker_totals.get(symbol, (0, 0))
            ledger_quantity = int(position.quantity)
            ledger_available = int(position.available_quantity)
            projected_quantity = int(projected_for_strategy.get(symbol, 0) or 0)
            if broker_quantity <= 0:
                dropped.append(
                    {
                        "symbol": symbol,
                        "ledger_quantity": ledger_quantity,
                        "ledger_available_quantity": ledger_available,
                        "broker_quantity": broker_quantity,
                        "broker_can_sell": broker_can_sell,
                        "projected_quantity": projected_quantity,
                        "reason": "missing_or_zero_broker_position",
                    }
                )
                continue
            capped_quantity = min(projected_quantity, broker_quantity)
            capped_available = min(ledger_available, broker_can_sell, capped_quantity)
            if capped_quantity <= 0:
                dropped.append(
                    {
                        "symbol": symbol,
                        "ledger_quantity": ledger_quantity,
                        "ledger_available_quantity": ledger_available,
                        "broker_quantity": broker_quantity,
                        "broker_can_sell": broker_can_sell,
                        "projected_quantity": projected_quantity,
                        "reason": "broker_quantity_cap_zero",
                    }
                )
                continue
            if capped_quantity != ledger_quantity or capped_available != ledger_available:
                capped.append(
                    {
                        "symbol": symbol,
                        "ledger_quantity": ledger_quantity,
                        "ledger_available_quantity": ledger_available,
                        "broker_quantity": broker_quantity,
                        "broker_can_sell": broker_can_sell,
                        "projected_quantity": projected_quantity,
                        "reconciled_quantity": capped_quantity,
                        "reconciled_available_quantity": capped_available,
                    }
                )
            reconciled[symbol] = position.model_copy(
                update={
                    "quantity": capped_quantity,
                    "available_quantity": capped_available,
                }
            )
        diagnostics = {
            "schema_version": "miniqmt_broker_position_reconciliation_v1",
            "status": "reconciled",
            "strategy_id": strategy_id,
            "binding_id": binding_id,
            "trade_date": trade_date.isoformat(),
            "ledger_position_count": len(ledger_positions),
            "broker_position_count": len(broker_totals),
            "reconciled_position_count": len(reconciled),
            "dropped_position_count": len(dropped),
            "capped_position_count": len(capped),
            "dropped_positions": dropped,
            "capped_positions": capped,
            "position_authority": "broker_positions",
            "broker_authoritative": True,
            "account_strategy_count": len(strategy_quantities),
            "projection_adjustments": list(projection.adjustments),
        }
        return reconciled, diagnostics

    @staticmethod
    def _resolve_local_sim_portfolio_id(binding: SimulationReleaseBinding) -> str:
        metadata = binding.binding_config_json.get("metadata") if isinstance(binding.binding_config_json, dict) else {}
        if isinstance(metadata, dict):
            for key in ("paper_v2_portfolio_id", "local_sim_portfolio_id", "portfolio_id"):
                value = str(metadata.get(key) or "").strip()
                if value:
                    return value
        if binding.broker_account_id:
            return binding.broker_account_id
        return binding.strategy_id

    def _resolve_local_sim_manifest(
        self,
        *,
        portfolio_manifest: StrategyPackageManifest | None,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> tuple[StrategyPackageManifest | None, dict[str, Any]]:
        """Resolve one immutable manifest identity for the complete LocalSIM context.

        Ordinary bindings remain pinned to the Paper v2 portfolio manifest.  The
        StrategyPackage repository is consulted only for an explicit unattended
        successor lineage whose predecessor identity is independently proven by
        the portfolio manifest and by the release/binding metadata written during
        the side-effect-free roll-forward.
        """

        try:
            self._validate_manifest_identity(
                manifest=portfolio_manifest,
                runtime_release=runtime_release,
                binding=binding,
            )
        except DataUnavailableError:
            binding_config = binding.binding_config_json if isinstance(binding.binding_config_json, dict) else {}
            binding_metadata = (
                binding_config.get("metadata") if isinstance(binding_config.get("metadata"), dict) else {}
            )
            if binding_metadata.get("manifest_identity_source") != "strategy_package_current_manifest":
                raise
            return self._load_local_sim_successor_manifest(
                portfolio_manifest=portfolio_manifest,
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
            )

        return portfolio_manifest, {
            "schema_version": "localsim_manifest_identity_resolution_v1",
            "source": "paper_v2_portfolio_frozen_manifest",
            "package_id": portfolio_manifest.package_id if portfolio_manifest is not None else None,
            "manifest_sha256": portfolio_manifest.manifest_sha256 if portfolio_manifest is not None else None,
            "strategy_package_revalidation_performed": False,
        }

    def _load_local_sim_successor_manifest(
        self,
        *,
        portfolio_manifest: StrategyPackageManifest | None,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> tuple[StrategyPackageManifest, dict[str, Any]]:
        lineage = self._validate_local_sim_successor_manifest_lineage(
            portfolio_manifest=portfolio_manifest,
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
        )
        try:
            raw_manifest = self._package_manifest_loader(binding.package_id)
        except (DataUnavailableError, RuntimeConfigInvalidError):
            raise
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "failed to load authoritative StrategyPackage manifest for LocalSim successor context",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "package_id": binding.package_id,
                    "manifest_sha256": binding.manifest_sha256,
                    "extends_binding_id": lineage["extends_binding_id"],
                    "extends_release_id": lineage["extends_release_id"],
                },
            ) from exc

        if raw_manifest is None:
            raise DataUnavailableError(
                "LocalSim successor context requires the authoritative StrategyPackage manifest",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "package_id": binding.package_id,
                    "manifest_sha256": binding.manifest_sha256,
                    "extends_binding_id": lineage["extends_binding_id"],
                    "extends_release_id": lineage["extends_release_id"],
                },
            )
        try:
            manifest = (
                raw_manifest
                if isinstance(raw_manifest, StrategyPackageManifest)
                else StrategyPackageManifest.model_validate(raw_manifest)
            )
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "authoritative StrategyPackage manifest is invalid for LocalSim successor context",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "package_id": binding.package_id,
                    "manifest_sha256": binding.manifest_sha256,
                },
            ) from exc

        self._validate_manifest_identity(
            manifest=manifest,
            runtime_release=runtime_release,
            binding=binding,
        )
        return manifest, {
            "schema_version": "localsim_manifest_identity_resolution_v1",
            "source": "strategy_package_current_manifest",
            "package_id": manifest.package_id,
            "manifest_sha256": manifest.manifest_sha256,
            "source_release_manifest_sha256": lineage["source_release_manifest_sha256"],
            "manifest_identity_changed": lineage["manifest_identity_changed"],
            "extends_binding_id": lineage["extends_binding_id"],
            "extends_release_id": lineage["extends_release_id"],
            "source_binding_readback_id": lineage["source_binding_readback_id"],
            "source_release_readback_id": lineage["source_release_readback_id"],
            "strategy_package_revalidation_performed": False,
        }

    def _validate_local_sim_successor_manifest_lineage(
        self,
        *,
        portfolio_manifest: StrategyPackageManifest | None,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> dict[str, Any]:
        binding_config = binding.binding_config_json if isinstance(binding.binding_config_json, dict) else {}
        binding_metadata = binding_config.get("metadata") if isinstance(binding_config.get("metadata"), dict) else {}
        release_config = (
            runtime_release.release_config_json if isinstance(runtime_release.release_config_json, dict) else {}
        )
        release_metadata = release_config.get("metadata") if isinstance(release_config.get("metadata"), dict) else {}
        validation_evidence = (
            runtime_release.validation_evidence if isinstance(runtime_release.validation_evidence, dict) else {}
        )
        manifest_evidence = (
            validation_evidence.get("manifest_identity")
            if isinstance(validation_evidence.get("manifest_identity"), dict)
            else {}
        )
        expected_trade_date = trade_date.isoformat()
        expected_purpose = "localsim_unattended_daily_roll_forward"
        extends_release_id = str(binding_metadata.get("extends_release_id") or "").strip()
        extends_binding_id = str(binding_metadata.get("extends_binding_id") or "").strip()
        source_manifest_sha256 = str(binding_metadata.get("source_release_manifest_sha256") or "").strip()
        authoritative_manifest_sha256 = str(binding_metadata.get("authoritative_manifest_sha256") or "").strip()
        manifest_identity_changed = source_manifest_sha256 != authoritative_manifest_sha256

        violations: list[str] = []

        def require_equal(name: str, actual: Any, expected: Any) -> None:
            if actual != expected:
                violations.append(name)

        def require_bool_equal(name: str, actual: Any, expected: bool) -> None:
            if not isinstance(actual, bool) or actual is not expected:
                violations.append(name)

        require_equal("broker_backend", binding.broker_backend, SimulationBrokerBackend.LOCAL_SIM)
        require_equal("binding.release_id", binding.release_id, runtime_release.release_id)
        require_equal("binding.release_hash", binding.release_hash, runtime_release.release_hash)
        require_equal("binding.package_id", binding.package_id, runtime_release.package_id)
        require_equal("binding.manifest_sha256", binding.manifest_sha256, runtime_release.manifest_sha256)
        require_equal("binding.metadata.purpose", binding_metadata.get("purpose"), expected_purpose)
        require_equal("release.metadata.purpose", release_metadata.get("purpose"), expected_purpose)
        require_equal("binding.metadata.broker_backend", binding_metadata.get("broker_backend"), binding.broker_backend.value)
        require_equal("binding.metadata.target_trade_date", binding_metadata.get("target_trade_date"), expected_trade_date)
        require_equal("release.metadata.target_trade_date", release_metadata.get("target_trade_date"), expected_trade_date)
        require_equal(
            "binding.metadata.manifest_identity_source",
            binding_metadata.get("manifest_identity_source"),
            "strategy_package_current_manifest",
        )
        require_equal(
            "release.metadata.manifest_identity_source",
            release_metadata.get("manifest_identity_source"),
            "strategy_package_current_manifest",
        )
        require_bool_equal(
            "binding.metadata.manifest_identity_changed",
            binding_metadata.get("manifest_identity_changed"),
            manifest_identity_changed,
        )
        require_bool_equal(
            "release.metadata.manifest_identity_changed",
            release_metadata.get("manifest_identity_changed"),
            manifest_identity_changed,
        )
        require_equal("binding.metadata.new_release_id", binding_metadata.get("new_release_id"), runtime_release.release_id)
        require_equal("binding.metadata.extends_release_id", extends_release_id, runtime_release.base_release_id)
        require_equal("release.metadata.extends_release_id", release_metadata.get("extends_release_id"), extends_release_id)
        require_equal("release.metadata.extends_binding_id", release_metadata.get("extends_binding_id"), extends_binding_id)
        require_equal(
            "release.metadata.source_release_manifest_sha256",
            release_metadata.get("source_release_manifest_sha256"),
            source_manifest_sha256,
        )
        require_equal(
            "release.metadata.authoritative_manifest_sha256",
            release_metadata.get("authoritative_manifest_sha256"),
            authoritative_manifest_sha256,
        )
        require_equal("binding.metadata.authoritative_manifest_sha256", authoritative_manifest_sha256, binding.manifest_sha256)
        require_equal("validation_evidence.target_trade_date", validation_evidence.get("target_trade_date"), expected_trade_date)
        require_equal("validation_evidence.extends_release_id", validation_evidence.get("extends_release_id"), extends_release_id)
        require_equal("validation_evidence.extends_binding_id", validation_evidence.get("extends_binding_id"), extends_binding_id)
        require_equal("validation_evidence.manifest_identity.source", manifest_evidence.get("source"), "strategy_package_current_manifest")
        require_equal(
            "validation_evidence.manifest_identity.source_release_manifest_sha256",
            manifest_evidence.get("source_release_manifest_sha256"),
            source_manifest_sha256,
        )
        require_equal(
            "validation_evidence.manifest_identity.authoritative_manifest_sha256",
            manifest_evidence.get("authoritative_manifest_sha256"),
            authoritative_manifest_sha256,
        )
        require_bool_equal(
            "validation_evidence.manifest_identity.identity_changed",
            manifest_evidence.get("identity_changed"),
            manifest_identity_changed,
        )
        require_equal(
            "validation_evidence.manifest_identity.strategy_package_revalidation_performed",
            manifest_evidence.get("strategy_package_revalidation_performed"),
            False,
        )

        if not extends_release_id:
            violations.append("binding.metadata.extends_release_id.required")
        if not extends_binding_id or extends_binding_id == binding.binding_id:
            violations.append("binding.metadata.extends_binding_id.invalid")
        if not source_manifest_sha256:
            violations.append("binding.metadata.source_release_manifest_sha256.required")
        if portfolio_manifest is None:
            violations.append("portfolio.frozen_manifest.required")
        else:
            require_equal("portfolio.manifest.package_id", portfolio_manifest.package_id, binding.package_id)

        if violations:
            raise RuntimeConfigInvalidError(
                "LocalSim authoritative manifest successor lineage is invalid",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "package_id": binding.package_id,
                    "binding_manifest_sha256": binding.manifest_sha256,
                    "portfolio_manifest_sha256": (
                        portfolio_manifest.manifest_sha256 if portfolio_manifest is not None else None
                    ),
                    "violations": sorted(set(violations)),
                },
            )

        source_readback = self._validate_local_sim_successor_source_records(
            runtime_release=runtime_release,
            binding=binding,
            extends_release_id=extends_release_id,
            extends_binding_id=extends_binding_id,
            source_manifest_sha256=source_manifest_sha256,
        )

        return {
            "extends_binding_id": extends_binding_id,
            "extends_release_id": extends_release_id,
            "source_release_manifest_sha256": source_manifest_sha256,
            "authoritative_manifest_sha256": authoritative_manifest_sha256,
            "manifest_identity_changed": manifest_identity_changed,
            "source_release_readback_id": source_readback["source_release_id"],
            "source_binding_readback_id": source_readback["source_binding_id"],
        }

    def _validate_local_sim_successor_source_records(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        extends_release_id: str,
        extends_binding_id: str,
        source_manifest_sha256: str,
    ) -> dict[str, str]:
        repository = self._runtime_repository or SimulationRuntimeRepository()
        try:
            source_release = repository.get_strategy_runtime_release(extends_release_id)
            source_binding = repository.get_simulation_release_binding(extends_binding_id)
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "failed to read LocalSim successor source release and binding",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "extends_binding_id": extends_binding_id,
                    "extends_release_id": extends_release_id,
                },
            ) from exc

        if source_release is None or source_binding is None:
            raise DataUnavailableError(
                "LocalSim successor source release and binding are unavailable",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "extends_binding_id": extends_binding_id,
                    "extends_release_id": extends_release_id,
                    "source_release_found": source_release is not None,
                    "source_binding_found": source_binding is not None,
                },
            )

        violations: list[str] = []

        def require_equal(name: str, actual: Any, expected: Any) -> None:
            if actual != expected:
                violations.append(name)

        require_equal("source_release.release_id", source_release.release_id, extends_release_id)
        require_equal("source_binding.binding_id", source_binding.binding_id, extends_binding_id)
        require_equal("source_binding.release_id", source_binding.release_id, source_release.release_id)
        require_equal("source_binding.release_hash", source_binding.release_hash, source_release.release_hash)
        require_equal("source_release.package_id", source_release.package_id, runtime_release.package_id)
        require_equal("source_binding.package_id", source_binding.package_id, runtime_release.package_id)
        require_equal("source_release.manifest_sha256", source_release.manifest_sha256, source_manifest_sha256)
        require_equal("source_binding.manifest_sha256", source_binding.manifest_sha256, source_manifest_sha256)
        require_equal("source_binding.strategy_id", source_binding.strategy_id, binding.strategy_id)
        require_equal("source_binding.broker_backend", source_binding.broker_backend, SimulationBrokerBackend.LOCAL_SIM)
        require_equal(
            "source_binding.portfolio_id",
            self._resolve_local_sim_portfolio_id(source_binding),
            self._resolve_local_sim_portfolio_id(binding),
        )

        if violations:
            raise RuntimeConfigInvalidError(
                "LocalSim authoritative manifest successor source records are inconsistent",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "extends_binding_id": extends_binding_id,
                    "extends_release_id": extends_release_id,
                    "source_release_manifest_sha256": source_manifest_sha256,
                    "violations": sorted(set(violations)),
                },
            )

        return {
            "source_release_id": source_release.release_id,
            "source_binding_id": source_binding.binding_id,
        }

    def _load_strategy_package_manifest(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
    ) -> StrategyPackageManifest:
        try:
            raw_manifest = self._package_manifest_loader(binding.package_id)
        except (DataUnavailableError, RuntimeConfigInvalidError):
            raise
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "failed to load StrategyPackage manifest for MiniQMT simulation context",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "package_id": binding.package_id,
                    "manifest_sha256": binding.manifest_sha256,
                },
            ) from exc

        if raw_manifest is None:
            raise DataUnavailableError(
                "MiniQMT simulation context requires a frozen StrategyPackage manifest",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "package_id": binding.package_id,
                    "manifest_sha256": binding.manifest_sha256,
                },
            )
        manifest = (
            raw_manifest
            if isinstance(raw_manifest, StrategyPackageManifest)
            else StrategyPackageManifest.model_validate(raw_manifest)
        )
        self._validate_manifest_identity(
            manifest=manifest,
            runtime_release=runtime_release,
            binding=binding,
        )
        return manifest

    @staticmethod
    def _validate_manifest_identity(
        *,
        manifest: StrategyPackageManifest | None,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
    ) -> None:
        if manifest is None:
            return
        if manifest.package_id != binding.package_id or manifest.package_id != runtime_release.package_id:
            raise DataUnavailableError(
                "LocalSim manifest package_id does not match runtime release binding",
                context={
                    "manifest_package_id": manifest.package_id,
                    "release_package_id": runtime_release.package_id,
                    "binding_package_id": binding.package_id,
                },
            )
        if manifest.manifest_sha256 != binding.manifest_sha256 or manifest.manifest_sha256 != runtime_release.manifest_sha256:
            raise DataUnavailableError(
                "LocalSim manifest hash does not match runtime release binding",
                context={
                    "manifest_sha256": manifest.manifest_sha256,
                    "release_manifest_sha256": runtime_release.manifest_sha256,
                    "binding_manifest_sha256": binding.manifest_sha256,
                },
            )

    @staticmethod
    def _build_dependency(
        factory: Callable[[], Any],
        dependency_name: str,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> Any:
        try:
            return factory()
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                f"failed to construct {dependency_name} for production simulation context",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "broker_backend": binding.broker_backend.value,
                    "trade_date": trade_date.isoformat(),
                },
            ) from exc


class PreviewOnlyMiniQMTManagedOrderService:
    """Managed-order facade that persists preview evidence without calling MiniQMT."""

    preview_only = True

    def __init__(self, wrapped: QmtManagedOrderService) -> None:
        self._wrapped = wrapped
        self._repository = getattr(wrapped, "_repository", None)
        # Preview mode may read broker positions for SELL availability and
        # reconciliation, but it must never call place_order/cancel_order.
        self._broker = getattr(wrapped, "_broker", None)

    def preview_order(self, request: Any) -> Any:
        return self._wrapped.preview_order(request)

    def _broker_can_sell(self, symbol: str) -> int:
        helper = getattr(self._wrapped, "_broker_can_sell", None)
        if callable(helper):
            return int(helper(symbol))
        return 0

    def submit_batch(self, requests: list[Any]) -> Any:
        requests_list = list(requests)
        if not requests_list:
            raise DataUnavailableError("MiniQMT preview-only submit requires at least one request")
        batch_id = _preview_batch_id(requests_list)
        existing_result = self._existing_preview_result(batch_id, request_count=len(requests_list))
        if existing_result is not None:
            return existing_result
        preflights = self._preview_batch_preflight(requests_list)
        results = tuple(
            self._preview_submit_result(request, preflight)
            for request, preflight in zip(requests_list, preflights, strict=True)
        )
        succeeded = sum(1 for result in results if result.success)
        failed = len(results) - succeeded
        preview_result = MiniQMTPreviewBatchSubmitResult(
            success=failed == 0,
            total=len(results),
            succeeded=succeeded,
            failed=failed,
            results=results,
            compensation_required=False,
            compensation_hint=None,
            batch_id=batch_id,
            batch_status="PREVIEW_SUCCEEDED" if failed == 0 else "PREVIEW_FAILED",
            preflight_passed=failed == 0,
            retry_of_batch_id=None,
            compensation_actions=(),
        )
        return self._persist_preview_result(batch_id=batch_id, requests=requests_list, result=preview_result)

    def _preview_batch_preflight(self, requests: list[Any]) -> tuple[Any, ...]:
        helper = getattr(self._wrapped, "_batch_preflight", None)
        if callable(helper):
            return tuple(helper(requests))
        return tuple(self._wrapped.preview_order(request) for request in requests)

    def _preview_submit_result(self, request: Any, preflight: Any) -> Any:
        broker_can_sell = None
        if getattr(request, "order_type", None) == SELL_ORDER_TYPE:
            broker_can_sell = self._broker_can_sell(str(getattr(request, "symbol", "")))
            if broker_can_sell < int(getattr(request, "quantity", 0) or 0):
                errors = tuple(getattr(preflight, "errors", ()) or ()) + (
                    OrderPreflightError(
                        "INSUFFICIENT_BROKER_CAN_SELL",
                        "MiniQMT account-level can_sell is insufficient",
                        {
                            "broker_can_sell": broker_can_sell,
                            "requested_quantity": int(getattr(request, "quantity", 0) or 0),
                        },
                    ),
                )
                preflight = replace(preflight, allowed=False, errors=errors, broker_can_sell=broker_can_sell)
            else:
                preflight = replace(preflight, broker_can_sell=broker_can_sell)
        success = bool(getattr(preflight, "allowed", False))
        message = "preview-only dry-run accepted" if success else "preview-only preflight failed"
        return MiniQMTPreviewOrderSubmitResult(
            success=success,
            intent_id=None,
            qmt_order_id=None,
            broker_message=message,
            preflight=preflight,
            broker_called=False,
        )

    def _existing_preview_result(self, batch_id: str, *, request_count: int) -> Any | None:
        get_batch = getattr(self._repository, "get_order_batch", None)
        if get_batch is None:
            return None
        batch = get_batch(batch_id)
        if batch is None or not (batch.metadata or {}).get("preview_only"):
            return None
        result_json = batch.result_json if isinstance(batch.result_json, dict) else {}
        stored_results = tuple(
            _preview_result_from_payload(item)
            for item in result_json.get("results", ())
            if isinstance(item, dict)
        )
        if not stored_results:
            return None
        succeeded = sum(1 for item in stored_results if item.success)
        failed = sum(1 for item in stored_results if not item.success) + max(request_count - len(stored_results), 0)
        return MiniQMTPreviewBatchSubmitResult(
            success=failed == 0,
            total=request_count,
            succeeded=succeeded,
            failed=failed,
            results=stored_results,
            compensation_required=False,
            compensation_hint=None,
            batch_id=batch_id,
            batch_status=str(result_json.get("batch_status") or batch.metadata.get("preview_batch_status") or ""),
            preflight_passed=bool(result_json.get("preflight_passed", failed == 0)),
            retry_of_batch_id=batch_id,
            compensation_actions=(),
        )

    def _persist_preview_result(
        self,
        *,
        batch_id: str,
        requests: list[Any],
        result: "MiniQMTPreviewBatchSubmitResult",
    ) -> "MiniQMTPreviewBatchSubmitResult":
        if self._repository is None or getattr(self._repository, "upsert_order_batch", None) is None:
            return result

        persisted_results: list[MiniQMTPreviewOrderSubmitResult] = []
        self._upsert_preview_batch(batch_id=batch_id, requests=requests, result=result)
        if result.success:
            for request, item in zip(requests, result.results, strict=True):
                intent_id = self._create_preview_intent(
                    batch_id=batch_id,
                    request=request,
                    result=item,
                )
                persisted_results.append(replace(item, intent_id=intent_id))
        else:
            persisted_results.extend(result.results)

        persisted = replace(result, results=tuple(persisted_results))
        self._upsert_preview_batch(batch_id=batch_id, requests=requests, result=persisted)
        return persisted

    def _create_preview_intent(
        self,
        *,
        batch_id: str,
        request: Any,
        result: "MiniQMTPreviewOrderSubmitResult",
    ) -> str | None:
        create_intent = getattr(self._repository, "create_order_intent", None)
        if create_intent is None or not result.success:
            return None
        strategy_id = getattr(result.preflight, "strategy_id", None)
        if not strategy_id:
            return None
        metadata = dict(getattr(request, "metadata", {}) or {})
        metadata.update(
            {
                "source": "simulation_runtime_preview_only",
                "preview_only": True,
                "broker_called": False,
                "preview_batch_id": batch_id,
            }
        )
        intent = OrderIntentRecord(
            intent_id=new_qmt_id("qmtpreviewintent"),
            batch_id=batch_id,
            strategy_id=strategy_id,
            strategy_name=str(getattr(request, "strategy_name")),
            symbol=str(getattr(request, "symbol")),
            side=str(getattr(request, "side")),
            order_type=int(getattr(request, "order_type")),
            quantity=int(getattr(request, "quantity")),
            price_type=int(getattr(request, "price_type")),
            order_remark=str(getattr(request, "order_remark")),
            account_id=str(getattr(request, "account_id")),
            trade_date=getattr(request, "trade_date"),
            package_id=getattr(request, "package_id", None),
            selection_run_id=getattr(request, "selection_run_id", None),
            limit_price=getattr(request, "price", None),
            target_weight=getattr(request, "target_weight", None),
            estimated_notional=getattr(result.preflight, "estimated_notional", None),
            estimated_fee=getattr(result.preflight, "estimated_fee", None),
            preflight_status=IntentPreflightStatus.PASSED,
            submit_status=IntentSubmitStatus.CREATED,
            metadata=metadata,
            submitted_at=None,
        )
        return create_intent(intent).intent_id

    def _upsert_preview_batch(
        self,
        *,
        batch_id: str,
        requests: list[Any],
        result: "MiniQMTPreviewBatchSubmitResult",
    ) -> None:
        upsert = getattr(self._repository, "upsert_order_batch", None)
        if upsert is None:
            return
        get_batch = getattr(self._repository, "get_order_batch", lambda _batch_id: None)
        existing = get_batch(batch_id)
        created_at = existing.created_at if existing is not None else datetime.now(UTC)
        first_request = requests[0]
        strategy_ids = sorted(
            {
                item.preflight.strategy_id
                for item in result.results
                if getattr(item.preflight, "strategy_id", None)
            }
        )
        batch_status = OrderBatchStatus.CREATED if result.success else OrderBatchStatus.PREFLIGHT_FAILED
        result_json = result.to_dict()
        result_json.update(
            {
                "preview_only": True,
                "broker_called": False,
                "batch_status": result.batch_status,
                "preflight_passed": result.preflight_passed,
            }
        )
        upsert(
            OrderBatchRecord(
                batch_id=batch_id,
                strategy_id=strategy_ids[0] if len(strategy_ids) == 1 else None,
                account_id=str(getattr(first_request, "account_id")),
                mode=str(getattr(first_request, "mode", "SIM") or "SIM"),
                batch_status=batch_status,
                request_json={"orders": [_preview_request_signature(request) for request in requests]},
                result_json=result_json,
                metadata={
                    "source": "simulation_runtime_preview_only",
                    "preview_only": True,
                    "preview_batch_status": result.batch_status,
                    "preflight_passed": result.preflight_passed,
                    "broker_called": False,
                    "mini_qmt_submit_enabled": False,
                },
                created_at=created_at,
                submitted_at=None,
                completed_at=datetime.now(UTC),
            )
        )


@dataclass(frozen=True)
class MiniQMTPreviewOrderSubmitResult:
    success: bool
    intent_id: str | None
    qmt_order_id: str | None
    broker_message: str
    preflight: Any
    broker_called: bool

    def to_dict(self) -> dict[str, Any]:
        preflight_dict = self.preflight.to_dict() if hasattr(self.preflight, "to_dict") else dict(self.preflight)
        return {
            "success": self.success,
            "intent_id": self.intent_id,
            "qmt_order_id": self.qmt_order_id,
            "broker_message": self.broker_message,
            "preflight": preflight_dict,
            "broker_called": self.broker_called,
            "preview_only": True,
        }


@dataclass(frozen=True)
class MiniQMTPreviewBatchSubmitResult:
    success: bool
    total: int
    succeeded: int
    failed: int
    results: tuple[MiniQMTPreviewOrderSubmitResult, ...]
    compensation_required: bool
    compensation_hint: str | None = None
    batch_id: str | None = None
    batch_status: str | None = None
    preflight_passed: bool = True
    retry_of_batch_id: str | None = None
    compensation_actions: tuple[dict[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "batch_id": self.batch_id,
            "batch_status": self.batch_status,
            "preflight_passed": self.preflight_passed,
            "retry_of_batch_id": self.retry_of_batch_id,
            "total": self.total,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "results": [result.to_dict() for result in self.results],
            "compensation_required": self.compensation_required,
            "compensation_hint": self.compensation_hint,
            "compensation_actions": list(self.compensation_actions),
            "preview_only": True,
        }


def _preview_batch_id(requests: list[Any]) -> str:
    signatures = [_preview_request_signature(request) for request in requests]
    return f"qmtpreview_{canonical_json_sha256(signatures)[:24]}"


def _preview_request_signature(request: Any) -> dict[str, Any]:
    metadata = getattr(request, "metadata", {}) or {}
    return {
        "account_id": str(getattr(request, "account_id", "")),
        "strategy_name": str(getattr(request, "strategy_name", "")),
        "symbol": str(getattr(request, "symbol", "")),
        "side": str(getattr(request, "side", "")),
        "order_type": int(getattr(request, "order_type", 0) or 0),
        "quantity": int(getattr(request, "quantity", 0) or 0),
        "price_type": int(getattr(request, "price_type", 0) or 0),
        "price": str(getattr(request, "price", "")),
        "order_remark": str(getattr(request, "order_remark", "")),
        "trade_date": getattr(request, "trade_date").isoformat()
        if getattr(request, "trade_date", None) is not None
        else None,
        "mode": str(getattr(request, "mode", "SIM") or "SIM"),
        "package_id": getattr(request, "package_id", None),
        "selection_run_id": getattr(request, "selection_run_id", None),
        "target_weight": str(getattr(request, "target_weight", ""))
        if getattr(request, "target_weight", None) is not None
        else None,
        "execution_plan_id": metadata.get("execution_plan_id"),
        "execution_plan_intent_id": metadata.get("execution_plan_intent_id"),
        "metadata": _json_safe_preview(metadata),
    }


def _preview_result_from_payload(payload: dict[str, Any]) -> MiniQMTPreviewOrderSubmitResult:
    return MiniQMTPreviewOrderSubmitResult(
        success=bool(payload.get("success")),
        intent_id=payload.get("intent_id"),
        qmt_order_id=payload.get("qmt_order_id"),
        broker_message=str(payload.get("broker_message") or "existing preview-only dry-run result"),
        preflight=dict(payload.get("preflight") or {}),
        broker_called=bool(payload.get("broker_called", False)),
    )


def _json_safe_preview(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe_preview(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe_preview(item) for item in value]
    if isinstance(value, date | datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def _default_price_loader(symbols: list[str], trade_date: date) -> dict[str, float]:
    unique_symbols = sorted({str(symbol).strip() for symbol in symbols if str(symbol).strip()})
    if not unique_symbols:
        return {}
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (ts_code)
                           ts_code, trade_date, close_li
                    FROM market.kline_daily_raw
                    WHERE ts_code = ANY(%s)
                      AND trade_date <= %s
                      AND close_li IS NOT NULL
                      AND close_li > 0
                    ORDER BY ts_code, trade_date DESC
                    """,
                    (unique_symbols, trade_date),
                )
                rows = cur.fetchall()
    except Exception as exc:  # noqa: BLE001
        raise DataUnavailableError(
            "failed to load latest market close prices for production simulation context",
            context={"symbols": unique_symbols, "trade_date": trade_date.isoformat()},
        ) from exc
    prices = {str(row["ts_code"]): float(row["close_li"]) / 1000.0 for row in rows}
    missing = sorted(symbol for symbol in unique_symbols if symbol not in prices)
    if missing:
        raise DataUnavailableError(
            "missing market close prices for production simulation context",
            context={"missing_symbols": missing, "trade_date": trade_date.isoformat()},
        )
    return prices


def _default_paper_repository_factory() -> Any:
    from backend.services.paper_trading_v2.repository import PaperTradingV2Repository

    return PaperTradingV2Repository()


def _default_qmt_repository_factory() -> Any:
    from backend.services.qmt_strategy_ledger.repository import QmtStrategyLedgerRepository

    return QmtStrategyLedgerRepository()


def _default_qmt_client_factory() -> Any:
    from backend.infra.qmt_client import get_qmt_client_singleton

    return get_qmt_client_singleton()


def _default_qmt_calendar_provider() -> Any:
    from backend.services.qmt_strategy_ledger.lot_availability import DbTradingCalendarProvider

    return DbTradingCalendarProvider()


def _default_strategy_package_manifest_loader(package_id: str) -> StrategyPackageManifest:
    from backend.services.strategy_package.repository import StrategyPackageRepository

    return StrategyPackageRepository().get(package_id).current_manifest()


def _broker_position_totals(positions: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> dict[str, tuple[int, int]]:
    totals: dict[str, tuple[int, int]] = {}
    for position in positions:
        symbol = str(position.get("stock_code") or position.get("symbol") or "").strip()
        if not symbol:
            continue
        quantity = _safe_non_negative_int(position.get("quantity", position.get("volume", 0)))
        can_sell = _safe_non_negative_int(position.get("can_sell", position.get("can_use_volume", 0)))
        existing_quantity, existing_can_sell = totals.get(symbol, (0, 0))
        totals[symbol] = (existing_quantity + quantity, existing_can_sell + can_sell)
    return dict(sorted(totals.items()))


def _miniqmt_reconciliation_diagnostic_adjustment_symbols(diagnostics: Any) -> set[str]:
    if not isinstance(diagnostics, dict):
        return set()
    symbols: set[str] = set()
    for key in ("dropped_positions", "capped_positions"):
        rows = diagnostics.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or "").strip()
            if symbol:
                symbols.add(symbol)
    return symbols


def _safe_non_negative_int(value: Any) -> int:
    try:
        return max(int(value or 0), 0)
    except (TypeError, ValueError):
        return 0


def _env_flag(name: str, *, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def _context_provider_status(provider: Any) -> dict[str, Any]:
    status_fn = getattr(provider, "status", None)
    if callable(status_fn):
        try:
            payload = dict(status_fn())
        except Exception as exc:  # noqa: BLE001
            payload = {
                "provider_mode": getattr(provider, "provider_mode", "unknown"),
                "provider_name": type(provider).__name__,
                "ready": False,
                "diagnostic": f"context provider status failed: {type(exc).__name__}: {exc}",
            }
    else:
        payload = {
            "provider_mode": getattr(provider, "provider_mode", type(provider).__name__),
            "provider_name": type(provider).__name__,
            "ready": True,
        }
    payload.setdefault("provider_name", type(provider).__name__)
    payload.setdefault("provider_mode", getattr(provider, "provider_mode", type(provider).__name__))
    return payload


def build_simulation_lifecycle_scheduler_from_env(
    *,
    repository: SimulationRuntimeRepository | InMemorySimulationRuntimeRepository | Any | None = None,
) -> SimulationLifecycleScheduler:
    resolved_repository = repository or SimulationRuntimeRepository()
    mode = (os.getenv("SIMULATION_RUNTIME_CONTEXT_PROVIDER") or "").strip().lower()
    production_enabled = _env_flag("ENABLE_SIMULATION_RUNTIME_PRODUCTION_PROVIDER", default=False)
    if mode in {"production", "prod"} or production_enabled:
        provider: SimulationRunContextProvider = ProductionSimulationRunContextProvider(
            runtime_repository=resolved_repository,
        )
    else:
        provider = FailFastSimulationRunContextProvider()
    quote_ingress_activation = build_miniqmt_quote_ingress_activation_from_env()
    return SimulationLifecycleScheduler(
        repository=resolved_repository,
        context_provider=provider,
        trading_calendar_service=TradingCalendarStatusService(),
        miniqmt_quote_ingress_activation=quote_ingress_activation,
    )


@dataclass(frozen=True)
class SimulationSchedulerBindingResult:
    binding_id: str
    strategy_id: str
    broker_backend: SimulationBrokerBackend
    status: str
    run: SimulationDailyRun | None = None
    execution_plan: ExecutionPlan | None = None
    execution_result: SimulationExecutionResult | None = None
    sync_result: dict[str, Any] | None = None
    reconciliation_result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    lifecycle_diagnostic: dict[str, Any] | None = None
    data_source: str | None = None

    @property
    def is_success(self) -> bool:
        return self.error is None


@dataclass(frozen=True)
class SimulationSchedulerRunOnceResult:
    trade_date: date
    data_source: str
    submit: bool
    total_bindings: int
    results: tuple[SimulationSchedulerBindingResult, ...]
    stale_run_results: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    as_of_time: datetime | None = None
    schedule_windows: tuple[dict[str, Any], ...] = field(default_factory=tuple)

    @property
    def planned_count(self) -> int:
        return sum(1 for item in self.results if item.status == "PLANNED")

    @property
    def reused_count(self) -> int:
        return sum(1 for item in self.results if item.status == "REUSED_EXISTING_PLAN")

    @property
    def submitted_count(self) -> int:
        return sum(
            1
            for item in self.results
            if item.status
            in {
                "SUBMITTED",
                "RECONCILED",
                "RECONCILIATION_PENDING_OPEN_ORDERS",
                "TAIL_HANDLED",
                "RECONCILIATION_WARNING",
                "NO_REBALANCE",
            }
        )

    @property
    def failed_count(self) -> int:
        return sum(1 for item in self.results if item.error is not None)

    @property
    def stale_terminalized_count(self) -> int:
        return sum(1 for item in self.stale_run_results if item.get("terminalization_succeeded") is not False)

    @property
    def stale_recovery_failed_count(self) -> int:
        return sum(1 for item in self.stale_run_results if item.get("terminalization_succeeded") is False)


@dataclass
class _SelectionInferenceInFlight:
    key: tuple[Any, ...]
    future: Future
    started_monotonic: float
    started_at: str
    context: dict[str, Any]
    timed_out: bool = False


class SimulationLifecycleScheduler:
    """Run one unattended lifecycle tick for eligible simulation bindings."""

    def __init__(
        self,
        *,
        repository: SimulationRuntimeRepository | InMemorySimulationRuntimeRepository | Any | None = None,
        selection_service: StrategyPackageSelectionService | Any | None = None,
        orchestrator: SimulationLifecycleOrchestrator | None = None,
        context_provider: SimulationRunContextProvider | None = None,
        performance_service: StrategyPerformanceProjectionService | None = None,
        trading_calendar_service: Any | None = None,
        miniqmt_quote_context_adapter: Any | None = None,
        b0_quote_v2_controller_factory: Any | None = None,
        miniqmt_quote_ingress_activation: Any | None = None,
        selection_inference_timeout_seconds: float | None = None,
        selection_inference_max_workers: int | None = None,
    ) -> None:
        activation_factory = (
            getattr(miniqmt_quote_ingress_activation, "controller_factory", None)
            if miniqmt_quote_ingress_activation is not None
            else None
        )
        if (
            b0_quote_v2_controller_factory is not None
            and activation_factory is not None
            and b0_quote_v2_controller_factory is not activation_factory
        ):
            raise ValueError("scheduler quote activation and explicit B0_QUOTE_V2 controller factories conflict")
        effective_b0_factory = b0_quote_v2_controller_factory or activation_factory
        activation_context_adapter = (
            getattr(miniqmt_quote_ingress_activation, "quote_context_adapter", None)
            if miniqmt_quote_ingress_activation is not None
            else None
        )
        if (
            miniqmt_quote_context_adapter is not None
            and activation_context_adapter is not None
            and miniqmt_quote_context_adapter is not activation_context_adapter
        ):
            raise ValueError("scheduler quote activation and explicit quote context adapters conflict")
        self.repository = repository or SimulationRuntimeRepository()
        self.selection_service = selection_service or StrategyPackageSelectionService(repository=self.repository)
        self.orchestrator = orchestrator or SimulationLifecycleOrchestrator(
            repository=self.repository,
            b0_quote_v2_controller_factory=effective_b0_factory,
        )
        if orchestrator is not None and effective_b0_factory is not None:
            existing_factory = getattr(orchestrator, "b0_quote_v2_controller_factory", None)
            if existing_factory is not None and existing_factory is not effective_b0_factory:
                raise ValueError("scheduler and orchestrator B0_QUOTE_V2 controller factories conflict")
            orchestrator.b0_quote_v2_controller_factory = effective_b0_factory
        self.context_provider = context_provider or FailFastSimulationRunContextProvider()
        self.performance_service = performance_service or StrategyPerformanceProjectionService()
        self._selection_inference_timeout_seconds = (
            float(selection_inference_timeout_seconds)
            if selection_inference_timeout_seconds is not None
            else self._selection_inference_timeout_seconds_from_env()
        )
        if self._selection_inference_timeout_seconds <= 0:
            raise ValueError("selection_inference_timeout_seconds must be positive")
        max_workers = (
            int(selection_inference_max_workers)
            if selection_inference_max_workers is not None
            else self._selection_inference_max_workers_from_env()
        )
        if max_workers <= 0:
            raise ValueError("selection_inference_max_workers must be positive")
        self._selection_inference_executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="simulation-selection-inference",
        )
        self._selection_inference_lock = threading.RLock()
        self._selection_inference_inflight: dict[tuple[Any, ...], _SelectionInferenceInFlight] = {}
        self._selection_inference_shutdown = False
        if trading_calendar_service is not None:
            self.trading_calendar_service = trading_calendar_service
        elif isinstance(self.repository, InMemorySimulationRuntimeRepository):
            self.trading_calendar_service = None
        else:
            self.trading_calendar_service = TradingCalendarStatusService()
        self._miniqmt_quote_context_adapter = miniqmt_quote_context_adapter or activation_context_adapter
        self._miniqmt_quote_ingress_activation = miniqmt_quote_ingress_activation
        self._b0_quote_v2_controller_factory = effective_b0_factory

    def status(self) -> dict[str, Any]:
        provider_status = _context_provider_status(self.context_provider)
        return {
            "ok": True,
            "scheduler": "simulation_lifecycle_scheduler",
            "autostart": False,
            "default_submit": False,
            "approval_states": [state.value for state in DEFAULT_SCHEDULER_SIM_BINDING_STATES],
            "sim_binding_selection_policy": "all_non_retired",
            "manual_tick_endpoint_enabled": True,
            "scheduler_control_api_enabled": False,
            "context_provider": provider_status,
            "context_provider_mode": provider_status.get("provider_mode"),
            "schedule_windows": list(DEFAULT_SCHEDULER_WINDOWS),
            "schedule_timezone": SCHEDULER_TZ_NAME,
            "restart_recovery_mode": "persisted_state_only",
            "window_orchestration": {
                "pre_open": "readiness",
                "selection": "daily_selection_evidence",
                "planning": "execution_plan",
                "execution": "submit_and_reconcile",
                "post_close_reconcile": "post_close_terminalization",
            },
            "miniqmt_sim_runtime": {
                "sim_runtime_kind": MiniQMTExecutionRuntimeKind.EVENT_LOOP.value,
                "runtime_selector_effect": "event_loop_only_no_runtime_switch",
                "compiler_route_retired": True,
                "live_forbidden": True,
            },
            "miniqmt_quote_context": self._miniqmt_quote_context_health(),
            "miniqmt_quote_ingress_activation": self._miniqmt_quote_ingress_activation_health(),
            "b0_quote_v2_controllers": (
                self._b0_quote_v2_controller_factory.health()
                if self._b0_quote_v2_controller_factory is not None
                else {"status": "DISABLED", "controller_count": 0}
            ),
            "binding_watchdog": {
                "timeout_env_var": SIMULATION_BINDING_WATCHDOG_TIMEOUT_ENV,
                "timeout_seconds": self._timeout_seconds_from_env(
                    SIMULATION_BINDING_WATCHDOG_TIMEOUT_ENV,
                    DEFAULT_SIMULATION_BINDING_WATCHDOG_TIMEOUT_SECONDS,
                ),
                "miniqmt_submit_timeout_env_var": SIMULATION_MINIQMT_SUBMIT_TIMEOUT_ENV,
                "miniqmt_submit_timeout_seconds": self._timeout_seconds_from_env(
                    SIMULATION_MINIQMT_SUBMIT_TIMEOUT_ENV,
                    DEFAULT_MINIQMT_SUBMIT_TIMEOUT_SECONDS,
                ),
                "miniqmt_reconcile_timeout_env_var": SIMULATION_MINIQMT_RECONCILE_TIMEOUT_ENV,
                "miniqmt_reconcile_timeout_seconds": self._timeout_seconds_from_env(
                    SIMULATION_MINIQMT_RECONCILE_TIMEOUT_ENV,
                    DEFAULT_MINIQMT_RECONCILE_TIMEOUT_SECONDS,
                ),
                "miniqmt_tick_driver_timeout_env_var": SIMULATION_MINIQMT_TICK_DRIVER_TIMEOUT_ENV,
                "miniqmt_tick_driver_timeout_seconds": self._timeout_seconds_from_env(
                    SIMULATION_MINIQMT_TICK_DRIVER_TIMEOUT_ENV,
                    DEFAULT_MINIQMT_TICK_DRIVER_TIMEOUT_SECONDS,
                ),
            },
            "selection_inference": self._selection_inference_status(),
        }

    def refresh_miniqmt_quote_context(self, **kwargs: Any) -> Any:
        """Scheduler-only P1-C context seam; it never changes run/submit state."""

        adapter = self._miniqmt_quote_context_adapter
        preload = getattr(adapter, "preload", None)
        if not callable(preload):
            raise DataUnavailableError(
                "MiniQMT quote context adapter is not configured for this scheduler",
                context={"reason_code": "ADAPTIVE_IS_QUOTE_CLOCK_CALENDAR_INVALID", "stage": "CLOCK"},
            )
        return preload(**kwargs)

    def _refresh_miniqmt_quote_context_lifecycle(self) -> None:
        """Refresh read-only quote context without changing lifecycle or submit state."""

        adapter = self._current_miniqmt_quote_context_adapter()
        refresh = getattr(adapter, "refresh_lifecycle", None)
        if not callable(refresh):
            return
        registered_runtime_count = getattr(adapter, "registered_runtime_count", None)
        if (
            self._miniqmt_quote_ingress_activation is not None
            and callable(registered_runtime_count)
            and registered_runtime_count() == 0
        ):
            return
        clock_at_utc = datetime.now(UTC)
        clock_monotonic_ns = monotonic_time.monotonic_ns()
        try:
            refresh(clock_at_utc=clock_at_utc, clock_monotonic_ns=clock_monotonic_ns)
        except QuoteContractError as exc:
            logger.error("MiniQMT quote context lifecycle refresh failed loudly: %s", exc.as_loud_payload())
        except Exception as exc:  # The adapter fault is loud but never rewrites run status.
            logger.error(
                "MiniQMT quote context lifecycle refresh raised unexpectedly: exception_type=%s",
                type(exc).__name__,
                exc_info=True,
            )

    def _miniqmt_quote_context_health(self) -> dict[str, Any]:
        adapter = self._current_miniqmt_quote_context_adapter()
        if adapter is None:
            return {
                "status": "UNCONFIGURED",
                "scope": "P1-C read-only context seam; no runtime submit gate",
            }
        health = getattr(adapter, "health", None)
        if not callable(health):
            return {
                "status": "INVALID",
                "reason_code": "ADAPTIVE_IS_QUOTE_CLOCK_CALENDAR_INVALID",
                "stage": "CLOCK",
                "message": "configured MiniQMT quote context adapter has no read-only health method",
            }
        try:
            result = health()
        except Exception as exc:  # Health presentation cannot rewrite lifecycle state.
            logger.error(
                "MiniQMT quote context health lookup failed: exception_type=%s",
                type(exc).__name__,
                exc_info=True,
            )
            return {
                "status": "INVALID",
                "reason_code": "ADAPTIVE_IS_QUOTE_CLOCK_CALENDAR_INVALID",
                "stage": "CLOCK",
                "exception_type": type(exc).__name__,
            }
        if not isinstance(result, dict):
            return {
                "status": "INVALID",
                "reason_code": "ADAPTIVE_IS_QUOTE_CLOCK_CALENDAR_INVALID",
                "stage": "CLOCK",
                "message": "configured MiniQMT quote context health is not a mapping",
            }
        return dict(result)

    def _current_miniqmt_quote_context_adapter(self) -> Any | None:
        activation_adapter = (
            getattr(self._miniqmt_quote_ingress_activation, "quote_context_adapter", None)
            if self._miniqmt_quote_ingress_activation is not None
            else None
        )
        return activation_adapter or self._miniqmt_quote_context_adapter

    def _prepare_miniqmt_quote_context_for_plan(
        self,
        *,
        binding: SimulationReleaseBinding,
        plan: ExecutionPlan,
        as_of_time: datetime | None,
        recovering_active: bool,
    ) -> dict[str, Any] | None:
        if binding.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
            return None
        if plan.plan_payload_json.get("quote_control") is None:
            return None
        activation = self._miniqmt_quote_ingress_activation
        prepare = getattr(activation, "prepare_runtime_context", None)
        if not callable(prepare):
            raise RuntimeConfigInvalidError(
                "B0_QUOTE_V2 execution requires scheduler-owned authoritative context publication",
                context={
                    "reason_code": "ADAPTIVE_IS_QUOTE_CLOCK_CALENDAR_INVALID",
                    "stage": "CLOCK",
                    "plan_id": plan.plan_id,
                    "binding_id": binding.binding_id,
                    "broker_called": False,
                    "legacy_fallback": False,
                },
            )
        runtime_id = MiniQMTExecutionBridge._runtime_id(plan=plan, binding=binding)
        clock_at_utc = (
            as_of_time.astimezone(UTC)
            if isinstance(as_of_time, datetime) and as_of_time.tzinfo is not None
            else datetime.now(UTC)
        )
        return prepare(
            runtime_id=runtime_id,
            plan=plan,
            recovering_active=bool(recovering_active),
            clock_at_utc=clock_at_utc,
            clock_monotonic_ns=monotonic_time.monotonic_ns(),
        )

    def _miniqmt_quote_ingress_activation_health(self) -> dict[str, Any]:
        activation = self._miniqmt_quote_ingress_activation
        if activation is None:
            return {
                "schema_version": "miniqmt_quote_ingress_activation_v1",
                "status": "UNCONFIGURED",
                "factory_available": False,
            }
        health = getattr(activation, "health", None)
        if not callable(health):
            raise RuntimeConfigInvalidError(
                "configured MiniQMT quote ingress activation has no health method",
                context={
                    "reason_code": "MINIQMT_QUOTE_INGRESS_ACTIVATION_INVALID",
                    "stage": "MINIQMT_QUOTE_INGRESS_ACTIVATION_HEALTH",
                },
            )
        payload = health()
        if not isinstance(payload, dict):
            raise RuntimeConfigInvalidError(
                "MiniQMT quote ingress activation health must be a mapping",
                context={
                    "reason_code": "MINIQMT_QUOTE_INGRESS_ACTIVATION_INVALID",
                    "stage": "MINIQMT_QUOTE_INGRESS_ACTIVATION_HEALTH",
                },
            )
        return dict(payload)

    def _advance_miniqmt_quote_ingress_lifecycle(self) -> None:
        activation = self._miniqmt_quote_ingress_activation
        if activation is None:
            return
        begin_epoch = getattr(activation, "begin_lifecycle_epoch", None)
        if not callable(begin_epoch):
            raise RuntimeConfigInvalidError(
                "configured MiniQMT quote ingress activation lacks its scheduler lifecycle method",
                context={
                    "reason_code": "MINIQMT_QUOTE_INGRESS_ACTIVATION_INVALID",
                    "stage": "MINIQMT_QUOTE_INGRESS_ACTIVATION_LIFECYCLE",
                },
            )
        begin_epoch()

    def shutdown_miniqmt_quote_ingress(self) -> None:
        activation = self._miniqmt_quote_ingress_activation
        if activation is None:
            return
        shutdown = getattr(activation, "shutdown", None)
        if not callable(shutdown):
            raise RuntimeConfigInvalidError(
                "configured MiniQMT quote ingress activation has no shutdown method",
                context={
                    "reason_code": "MINIQMT_QUOTE_INGRESS_ACTIVATION_INVALID",
                    "stage": "MINIQMT_QUOTE_INGRESS_ACTIVATION_SHUTDOWN",
                },
            )
        shutdown()

    def shutdown_selection_inference(self, *, wait: bool = True) -> None:
        self._selection_inference_executor.shutdown(wait=wait, cancel_futures=not wait)
        with self._selection_inference_lock:
            self._selection_inference_shutdown = True

    def _selection_inference_status(self) -> dict[str, Any]:
        now = monotonic_time.monotonic()
        with self._selection_inference_lock:
            shutdown = self._selection_inference_shutdown
            in_flight = []
            for entry in self._selection_inference_inflight.values():
                in_flight.append(
                    {
                        **entry.context,
                        "started_at": entry.started_at,
                        "elapsed_seconds": round(max(0.0, now - entry.started_monotonic), 3),
                        "timeout_seconds": self._selection_inference_timeout_seconds,
                        "timed_out": entry.timed_out,
                        "done": entry.future.done(),
                    }
                )
        return {
            "mode": "artifact_hit_sync_else_background",
            "timeout_env_var": SIMULATION_SELECTION_INFERENCE_TIMEOUT_ENV,
            "max_workers_env_var": SIMULATION_SELECTION_INFERENCE_MAX_WORKERS_ENV,
            "timeout_seconds": self._selection_inference_timeout_seconds,
            "shutdown": shutdown,
            "in_flight_count": len(in_flight),
            "in_flight": in_flight,
        }

    @staticmethod
    def _selection_inference_timeout_seconds_from_env() -> float:
        raw = (os.getenv(SIMULATION_SELECTION_INFERENCE_TIMEOUT_ENV) or "300").strip()
        try:
            return float(raw)
        except ValueError as exc:
            raise ValueError(f"{SIMULATION_SELECTION_INFERENCE_TIMEOUT_ENV} must be a number") from exc

    @staticmethod
    def _selection_inference_max_workers_from_env() -> int:
        raw = (os.getenv(SIMULATION_SELECTION_INFERENCE_MAX_WORKERS_ENV) or "2").strip()
        try:
            return int(raw)
        except ValueError as exc:
            raise ValueError(f"{SIMULATION_SELECTION_INFERENCE_MAX_WORKERS_ENV} must be an integer") from exc

    @staticmethod
    def _timeout_seconds_from_env(env_var: str, default_seconds: float) -> float:
        raw = str(os.getenv(env_var) or "").strip()
        if not raw:
            return float(default_seconds)
        try:
            value = float(raw)
        except ValueError as exc:
            raise RuntimeConfigInvalidError(
                "simulation runtime timeout configuration must be numeric",
                context={
                    "reason_code": "SIMULATION_STAGE_TIMEOUT_CONFIG_INVALID",
                    "stage": "TIMEOUT_CONFIGURATION",
                    "env_var": env_var,
                    "raw_value": raw,
                },
            ) from exc
        if value <= 0:
            raise RuntimeConfigInvalidError(
                "simulation runtime timeout configuration must be positive",
                context={
                    "reason_code": "SIMULATION_STAGE_TIMEOUT_CONFIG_INVALID",
                    "stage": "TIMEOUT_CONFIGURATION",
                    "env_var": env_var,
                    "raw_value": raw,
                    "parsed_value": value,
                },
            )
        return value

    def _run_callable_with_timeout(
        self,
        *,
        stage: str,
        reason_code: str,
        timeout_env_var: str,
        default_timeout_seconds: float,
        context: dict[str, Any],
        func: Callable[[], Any],
    ) -> Any:
        timeout_seconds = self._timeout_seconds_from_env(timeout_env_var, default_timeout_seconds)
        result_queue: queue.Queue[tuple[str, Any]] = queue.Queue(maxsize=1)

        def target() -> None:
            try:
                result_queue.put(("result", func()))
            except BaseException as exc:  # noqa: BLE001 - propagate worker failures through the scheduler path.
                result_queue.put(("exception", exc))

        thread = threading.Thread(
            target=target,
            name=f"simulation-stage-{stage.lower().replace('_', '-')}",
            daemon=True,
        )
        thread.start()
        try:
            outcome, value = result_queue.get(timeout=timeout_seconds)
        except queue.Empty as exc:
            timeout_context = {
                **context,
                "reason_code": reason_code,
                "stage": stage,
                "failure_stage": stage,
                "timeout_env_var": timeout_env_var,
                "timeout_seconds": timeout_seconds,
                "thread_alive": thread.is_alive(),
            }
            raise RuntimeConfigInvalidError(
                f"simulation runtime stage timed out; reason_code={reason_code}, stage={stage}",
                context=timeout_context,
            ) from exc
        if outcome == "exception":
            raise value
        return value

    def run_once(
        self,
        *,
        trade_date: date,
        data_source: str,
        limit: int = 100,
        broker_backend: SimulationBrokerBackend | str | None = None,
        strategy_id: str | None = None,
        release_id: str | None = None,
        approval_states: tuple[SimulationBindingApprovalState, ...] | None = DEFAULT_SCHEDULER_SIM_BINDING_STATES,
        submit: bool = False,
        mode: str = "SIM",
        as_of_time: datetime | None = None,
        created_by: str = "simulation_lifecycle_scheduler",
        raise_on_error: bool = False,
    ) -> SimulationSchedulerRunOnceResult:
        if limit <= 0:
            raise ValueError("limit must be positive")
        self._ensure_lifecycle_trading_day(trade_date=trade_date)
        as_of_time = self._scheduler_time(as_of_time)
        self._refresh_miniqmt_quote_context_lifecycle()
        self._advance_miniqmt_quote_ingress_lifecycle()
        stale_run_results = self._run_recovery_stage_isolated(
            stage="STALE_MINIQMT_TERMINALIZATION",
            raise_on_error=raise_on_error,
            func=lambda: self._terminalize_stale_miniqmt_active_runs(
                trade_date=trade_date,
                broker_backend=broker_backend,
                strategy_id=strategy_id,
                limit=limit,
                as_of_time=as_of_time,
                raise_on_error=raise_on_error,
            ),
        )
        stale_run_results.extend(
            self._run_recovery_stage_isolated(
                stage="STALE_LOCALSIM_TERMINALIZATION",
                raise_on_error=raise_on_error,
                func=lambda: self._terminalize_stale_localsim_active_runs(
                    trade_date=trade_date,
                    broker_backend=broker_backend,
                    strategy_id=strategy_id,
                    limit=limit,
                    raise_on_error=raise_on_error,
                ),
            )
        )
        eod_terminalized_results = self._run_recovery_stage_isolated(
            stage="POST_CLOSE_MINIQMT_TERMINALIZATION",
            raise_on_error=raise_on_error,
            func=lambda: self._terminalize_post_close_miniqmt_runs(
                trade_date=trade_date,
                broker_backend=broker_backend,
                strategy_id=strategy_id,
                limit=limit,
                as_of_time=as_of_time,
                raise_on_error=raise_on_error,
            ),
        )
        eod_terminalized_results.extend(
            self._run_recovery_stage_isolated(
                stage="POST_CLOSE_LOCALSIM_TERMINALIZATION",
                raise_on_error=raise_on_error,
                func=lambda: self._terminalize_post_close_localsim_runs(
                    trade_date=trade_date,
                    broker_backend=broker_backend,
                    strategy_id=strategy_id,
                    limit=limit,
                    as_of_time=as_of_time,
                    raise_on_error=raise_on_error,
                ),
            )
        )
        bindings = self.repository.list_simulation_release_bindings(
            strategy_id=strategy_id,
            release_id=release_id,
            broker_backend=broker_backend,
            approval_states=approval_states,
            active_on=trade_date,
            limit=limit,
        )
        package_status_cache: dict[str, PackageStatus] = {}
        bindings, lifecycle_skips, blocked_binding_keys = self._partition_retired_package_bindings(
            bindings=bindings,
            data_source=data_source,
            package_status_cache=package_status_cache,
        )
        bindings = self._with_unattended_roll_forward_bindings(
            bindings=bindings,
            trade_date=trade_date,
            limit=limit,
            broker_backend=broker_backend,
            strategy_id=strategy_id,
            release_id=release_id,
            approval_states=approval_states,
            data_source=data_source,
            package_status_cache=package_status_cache,
            lifecycle_skips=lifecycle_skips,
            blocked_binding_keys=blocked_binding_keys,
        )
        results: list[SimulationSchedulerBindingResult] = list(lifecycle_skips)
        eod_terminalized_run_ids = {
            str(item.get("run_id"))
            for item in eod_terminalized_results
            if item.get("run_id") and item.get("terminalization_succeeded") is not False
        }
        selection_cache: dict[tuple[Any, ...], StrategyPackageSelectionResult | BaseException] = {}
        shared_selection_keys = self._shared_selection_cache_keys(
            bindings=bindings,
            trade_date=trade_date,
            data_source=data_source,
        )
        for binding in bindings:
            eod_result = self._post_close_terminalized_binding_result(
                binding=binding,
                trade_date=trade_date,
                data_source=data_source,
                run_ids=eod_terminalized_run_ids,
            )
            if eod_result is not None:
                results.append(eod_result)
                continue
            try:
                results.append(
                    self._run_binding_with_watchdog(
                        binding=binding,
                        trade_date=trade_date,
                        data_source=data_source,
                        submit=submit,
                        mode=mode,
                        created_by=created_by,
                        selection_cache=selection_cache,
                        shared_selection_keys=shared_selection_keys,
                        as_of_time=as_of_time,
                    )
                )
            except Exception as exc:  # noqa: BLE001 - isolate one binding without starving later eligible bindings.
                if raise_on_error:
                    raise
                results.append(
                    self._record_pre_run_binding_failure_result(
                        binding=binding,
                        trade_date=trade_date,
                        data_source=data_source,
                        created_by=created_by,
                        exc=exc,
                    )
                )
        return SimulationSchedulerRunOnceResult(
            trade_date=trade_date,
            data_source=data_source,
            submit=submit,
            total_bindings=len(bindings) + len(lifecycle_skips),
            results=tuple(results),
            stale_run_results=tuple([*stale_run_results, *eod_terminalized_results]),
            as_of_time=as_of_time,
            schedule_windows=self._compute_schedule_windows(trade_date=trade_date, as_of_time=as_of_time),
        )

    @staticmethod
    def _run_recovery_stage_isolated(
        *,
        stage: str,
        raise_on_error: bool,
        func: Callable[[], list[dict[str, Any]]],
    ) -> list[dict[str, Any]]:
        try:
            return func()
        except Exception as exc:  # noqa: BLE001 - recovery isolation is explicit and diagnostic.
            if raise_on_error:
                raise
            diagnostic = SimulationLifecycleScheduler._recovery_failure_diagnostic(stage=stage, exc=exc)
            logger.error("Simulation scheduler recovery stage failed without starving bindings: %s", diagnostic, exc_info=True)
            return [diagnostic]

    @staticmethod
    def _run_recovery_item_isolated(
        *,
        stage: str,
        run: SimulationDailyRun,
        raise_on_error: bool,
        func: Callable[[], dict[str, Any] | None],
    ) -> dict[str, Any] | None:
        try:
            return func()
        except Exception as exc:  # noqa: BLE001 - one bad durable run must not starve other runs or bindings.
            if raise_on_error:
                raise
            diagnostic = SimulationLifecycleScheduler._recovery_failure_diagnostic(stage=stage, exc=exc, run=run)
            logger.error("Simulation scheduler recovery item failed without starving peers: %s", diagnostic, exc_info=True)
            return diagnostic

    @staticmethod
    def _recovery_failure_diagnostic(
        *,
        stage: str,
        exc: Exception,
        run: SimulationDailyRun | None = None,
    ) -> dict[str, Any]:
        context = getattr(exc, "context", None)
        reason_code = "SIMULATION_SCHEDULER_RECOVERY_ITEM_FAILED" if run is not None else "SIMULATION_SCHEDULER_RECOVERY_STAGE_FAILED"
        diagnostic = {
            "schema_version": "simulation_scheduler_recovery_failure_v1",
            "terminalization_succeeded": False,
            "status": "RECOVERY_FAILED",
            "stage": stage,
            "reason_code": reason_code,
            "error": {
                "type": type(exc).__name__,
                "message": str(exc),
                "context": dict(context) if isinstance(context, dict) else context,
            },
            "alert": {
                "severity": "ERROR",
                "reason_code": reason_code,
                "stage": stage,
                "message": str(exc),
            },
        }
        if run is not None:
            diagnostic.update(
                {
                    "run_id": run.run_id,
                    "trade_date": run.trade_date.isoformat(),
                    "strategy_id": run.strategy_id,
                    "broker_backend": run.broker_backend.value,
                }
            )
        return diagnostic

    def _run_binding_with_watchdog(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        submit: bool,
        mode: str,
        created_by: str,
        selection_cache: dict[tuple[Any, ...], StrategyPackageSelectionResult | BaseException] | None,
        shared_selection_keys: set[tuple[Any, ...]] | None,
        as_of_time: datetime | None,
    ) -> SimulationSchedulerBindingResult:
        return self._run_callable_with_timeout(
            stage="BINDING_TICK",
            reason_code="SIMULATION_BINDING_STAGE_TIMEOUT",
            timeout_env_var=SIMULATION_BINDING_WATCHDOG_TIMEOUT_ENV,
            default_timeout_seconds=DEFAULT_SIMULATION_BINDING_WATCHDOG_TIMEOUT_SECONDS,
            context={
                "binding_id": binding.binding_id,
                "strategy_id": binding.strategy_id,
                "broker_backend": binding.broker_backend.value,
                "package_id": binding.package_id,
                "trade_date": trade_date.isoformat(),
                "data_source": data_source,
                "submit": bool(submit),
                "mode": str(mode or "SIM").strip().upper(),
            },
            func=lambda: self._run_binding(
                binding=binding,
                trade_date=trade_date,
                data_source=data_source,
                submit=submit,
                mode=mode,
                created_by=created_by,
                selection_cache=selection_cache,
                shared_selection_keys=shared_selection_keys,
                as_of_time=as_of_time,
            ),
        )

    def _record_pre_run_binding_failure_result(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        created_by: str,
        exc: Exception,
    ) -> SimulationSchedulerBindingResult:
        existing_after_failure = self.repository.get_simulation_daily_run_by_key(
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
            trade_date=trade_date,
        )
        submit_failure_recorded = (
            existing_after_failure is not None
            and bool(existing_after_failure.execution_plan_id)
            and isinstance(existing_after_failure.run_payload_json.get("submit_failure"), dict)
        )
        if (
            not submit_failure_recorded
            and not isinstance(exc, (DataUnavailableError, RuntimeConfigInvalidError))
            and self._has_broker_side_effect_evidence(
                binding=binding,
                trade_date=trade_date,
            )
        ):
            logger.exception(
                "Simulation lifecycle scheduler will not mark side-effect-bearing binding failure as pre-run failure",
                extra={
                    "binding_id": binding.binding_id,
                    "strategy_id": binding.strategy_id,
                    "broker_backend": binding.broker_backend.value,
                    "package_id": binding.package_id,
                    "error_type": type(exc).__name__,
                },
            )
            raise exc
        effective_data_source = self._effective_market_data_source_for_binding(
            binding=binding,
            trade_date=trade_date,
            default_data_source=data_source,
        )
        failed_run = self._persist_pre_run_binding_failure(
            binding=binding,
            trade_date=trade_date,
            data_source=effective_data_source,
            created_by=created_by,
            exc=exc,
        )
        if not isinstance(exc, (DataUnavailableError, RuntimeConfigInvalidError)):
            pre_run_failure = failed_run.run_payload_json.get("pre_run_failure")
            reason_code = (
                str(pre_run_failure.get("reason_code"))
                if isinstance(pre_run_failure, dict) and pre_run_failure.get("reason_code")
                else self._pre_run_failure_reason_code(exc, self._exception_context(exc))
            )
            failure_stage = (
                str(pre_run_failure.get("failure_stage"))
                if isinstance(pre_run_failure, dict) and pre_run_failure.get("failure_stage")
                else self._pre_run_failure_stage(exc, self._exception_context(exc))
            )
            logger.warning(
                "Simulation lifecycle scheduler isolated per-binding failure; continuing later bindings: %s",
                {
                    "binding_id": binding.binding_id,
                    "strategy_id": binding.strategy_id,
                    "broker_backend": binding.broker_backend.value,
                    "package_id": binding.package_id,
                    "reason_code": reason_code,
                    "failure_stage": failure_stage,
                    "error_type": type(exc).__name__,
                },
            )
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=failed_run.status.value,
            run=failed_run,
            error=self._pre_run_failure_error_payload(failed_run, exc=exc),
            data_source=effective_data_source,
        )

    def _has_broker_side_effect_evidence(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> bool:
        existing = self.repository.get_simulation_daily_run_by_key(
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
            trade_date=trade_date,
        )
        if existing is None:
            return False
        return self._run_has_broker_side_effect_evidence(existing)

    @staticmethod
    def _run_has_broker_side_effect_evidence(run: SimulationDailyRun) -> bool:
        payload = run.run_payload_json
        if bool(payload.get("broker_called")):
            return True
        if payload.get("miniqmt_side_effect_state") == "UNKNOWN_TIMEOUT":
            return True
        if isinstance(payload.get("miniqmt_submit_timeout"), dict):
            return True
        for key in ("submitted_intents", "failed_intents"):
            try:
                if int(payload.get(key) or 0) > 0:
                    return True
            except (TypeError, ValueError):
                return True
        return any(
            payload.get(key)
            for key in (
                "broker_order_handles",
                "qmt_batch_id",
                "qmt_batch_result",
                "local_sim_persistence",
                "reconcile_after_submit",
                "tail_handling",
            )
        )

    def _persist_pre_run_binding_failure(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        created_by: str,
        exc: Exception,
    ) -> SimulationDailyRun:
        runtime_release = self.repository.get_strategy_runtime_release(binding.release_id)
        existing = self.repository.get_simulation_daily_run_by_key(
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
            trade_date=trade_date,
        )
        identity = self._simulation_daily_run_identity(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
        )
        diagnostic = self._pre_run_failure_diagnostic(
            binding=binding,
            trade_date=trade_date,
            data_source=data_source,
            created_by=created_by,
            exc=exc,
        )
        if existing is None:
            digest = canonical_json_sha256(identity)
            existing = self.repository.save_simulation_daily_run(
                SimulationDailyRun(
                    run_id=f"simrun_{digest[:16]}",
                    trade_date=trade_date,
                    strategy_id=binding.strategy_id,
                    broker_backend=binding.broker_backend,
                    package_id=runtime_release.package_id,
                    manifest_sha256=runtime_release.manifest_sha256,
                    release_id=runtime_release.release_id,
                    release_hash=runtime_release.release_hash or "",
                    binding_id=binding.binding_id,
                    binding_hash=binding.binding_hash or "",
                    account_group_id=binding.account_group_id,
                    strategy_slot_id=binding.strategy_slot_id,
                    status=SimulationDailyRunStatus.FAILED_RETRYABLE,
                    run_payload_json={**identity, "created_by": created_by},
                )
            )
        if not self._is_pre_run_failure_run(existing):
            terminal_statuses = {
                SimulationDailyRunStatus.SUCCEEDED,
                SimulationDailyRunStatus.FAILED_TERMINAL,
                SimulationDailyRunStatus.CANCELLED,
            }
            if existing.execution_plan_id and isinstance(existing.run_payload_json.get("submit_failure"), dict):
                return existing
            if (
                not isinstance(exc, (DataUnavailableError, RuntimeConfigInvalidError))
                and existing.status not in terminal_statuses
                and not self._run_has_broker_side_effect_evidence(existing)
            ):
                return self.repository.update_simulation_daily_run(
                    existing.run_id,
                    status=SimulationDailyRunStatus.FAILED_RETRYABLE,
                    payload_patch={
                        "last_stage": "PRE_RUN_FAILED",
                        "broker_called": False,
                        "submitted_intents": 0,
                        "failed_intents": 0,
                        "pre_run_failure": diagnostic,
                        "submit_failure": {
                            "stage": "PRE_RUN_FAILED",
                            "failure_stage": diagnostic.get("failure_stage"),
                            "type": diagnostic["error_type"],
                            "message": diagnostic["message"],
                            "context": diagnostic,
                        },
                    },
                )
            return existing
        diagnostic = self._with_pre_run_failure_observation(existing, diagnostic)
        terminal_statuses = {
            SimulationDailyRunStatus.SUCCEEDED,
            SimulationDailyRunStatus.FAILED_TERMINAL,
            SimulationDailyRunStatus.CANCELLED,
        }
        if existing.status in terminal_statuses:
            return self.repository.update_simulation_daily_run(
                existing.run_id,
                payload_patch={
                    "pre_run_failure_observed_after_terminal": diagnostic,
                    "pre_run_failure_last_observed_at": diagnostic["last_observed_at"],
                },
            )
        return self.repository.update_simulation_daily_run(
            existing.run_id,
            status=SimulationDailyRunStatus.FAILED_RETRYABLE,
            payload_patch={
                "last_stage": "PRE_RUN_FAILED",
                "broker_called": False,
                "submitted_intents": 0,
                "failed_intents": 0,
                "pre_run_failure": diagnostic,
                "submit_failure": {
                    "stage": "PRE_RUN_FAILED",
                    "failure_stage": diagnostic.get("failure_stage"),
                    "type": diagnostic["error_type"],
                    "message": diagnostic["message"],
                    "context": diagnostic,
                },
            },
        )

    @staticmethod
    def _is_pre_run_failure_run(run: SimulationDailyRun) -> bool:
        payload = run.run_payload_json
        if run.execution_plan_id:
            return False
        if isinstance(payload.get("pre_run_failure"), dict):
            return True
        if payload.get("broker_called") is not None:
            return False
        if isinstance(payload.get("submit_failure"), dict):
            return False
        if isinstance(payload.get("qmt_batch_result"), dict):
            return False
        if isinstance(payload.get("local_sim_persistence"), dict):
            return False
        return True

    @staticmethod
    def _simulation_daily_run_identity(
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> dict[str, Any]:
        return {
            "schema_version": "simulation_daily_run_identity_v1",
            "strategy_id": binding.strategy_id,
            "binding_id": binding.binding_id,
            "binding_hash": binding.binding_hash,
            "release_id": runtime_release.release_id,
            "release_hash": runtime_release.release_hash,
            "broker_backend": binding.broker_backend.value,
            "trade_date": trade_date.isoformat(),
        }

    @staticmethod
    def _pre_run_failure_diagnostic(
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        created_by: str,
        exc: Exception,
    ) -> dict[str, Any]:
        context = SimulationLifecycleScheduler._exception_context(exc)
        reason_code = SimulationLifecycleScheduler._pre_run_failure_reason_code(exc, context)
        failure_stage = SimulationLifecycleScheduler._pre_run_failure_stage(exc, context)
        blocked_check = SimulationLifecycleScheduler._pre_run_failure_blocked_check(context)
        blocked_context = SimulationLifecycleScheduler._pre_run_failure_blocked_check_context(context)
        missing_relative_paths = SimulationLifecycleScheduler._pre_run_failure_missing_relative_paths(
            context,
            blocked_context=blocked_context,
        )
        observed_at = datetime.now(UTC).isoformat()
        legacy_pre_run_error = isinstance(exc, (DataUnavailableError, RuntimeConfigInvalidError))
        diagnostic = {
            "schema_version": "simulation_pre_run_failure_v1",
            "stage": "PRE_RUN_FAILED" if legacy_pre_run_error else failure_stage,
            "lifecycle_stage": "PRE_RUN_FAILED",
            "failure_stage": failure_stage,
            "reason_code": reason_code,
            "reason": "simulation_runtime_unattended_pre_run_binding_failed",
            "strategy_id": binding.strategy_id,
            "binding_id": binding.binding_id,
            "broker_backend": binding.broker_backend.value,
            "package_id": binding.package_id,
            "release_id": binding.release_id,
            "release_hash": binding.release_hash,
            "manifest_sha256": binding.manifest_sha256,
            "trade_date": trade_date.isoformat(),
            "data_source": data_source,
            "created_by": created_by,
            "error_type": type(exc).__name__,
            "message": str(exc),
            "context": context,
            "first_observed_at": observed_at,
            "last_observed_at": observed_at,
            "observed_count": 1,
            "broker_called": False,
            "submitted_intents": 0,
            "failed_intents": 0,
            "next_action": (
                "fix the data/configuration dependency reported by reason_code and rerun the scheduler tick; "
                "no broker order was submitted before this failure"
            ),
        }
        if blocked_check:
            diagnostic["blocked_check"] = blocked_check
        if blocked_context:
            diagnostic["blocked_check_context"] = blocked_context
        if missing_relative_paths:
            diagnostic["missing_relative_paths"] = missing_relative_paths
        return diagnostic

    @staticmethod
    def _with_pre_run_failure_observation(
        existing: SimulationDailyRun,
        diagnostic: dict[str, Any],
    ) -> dict[str, Any]:
        previous = existing.run_payload_json.get("pre_run_failure")
        if not isinstance(previous, dict):
            return diagnostic
        try:
            observed_count = int(previous.get("observed_count") or 0) + 1
        except (TypeError, ValueError):
            observed_count = 1
        return {
            **diagnostic,
            "first_observed_at": str(previous.get("first_observed_at") or diagnostic["first_observed_at"]),
            "observed_count": observed_count,
        }

    @staticmethod
    def _pre_run_failure_error_payload(
        run: SimulationDailyRun,
        *,
        exc: Exception | None = None,
    ) -> dict[str, Any]:
        pre_run_failure = run.run_payload_json.get("pre_run_failure")
        if isinstance(pre_run_failure, dict):
            return {
                "type": str(pre_run_failure.get("error_type") or run.status.value),
                "message": str(pre_run_failure.get("message") or run.status.value),
                "context": SimulationLifecycleScheduler._flatten_pre_run_failure_context(pre_run_failure),
            }
        terminal_failure = run.run_payload_json.get("pre_run_failure_observed_after_terminal")
        if isinstance(terminal_failure, dict):
            return {
                "type": str(terminal_failure.get("error_type") or run.status.value),
                "message": str(terminal_failure.get("message") or run.status.value),
                "context": SimulationLifecycleScheduler._flatten_pre_run_failure_context(terminal_failure),
            }
        if exc is not None:
            context = getattr(exc, "context", None)
            return {
                "type": type(exc).__name__,
                "message": str(exc),
                "context": context if isinstance(context, dict) else {"run_id": run.run_id},
            }
        failure = run.run_payload_json.get("submit_failure")
        if isinstance(failure, dict):
            context = failure.get("context") if isinstance(failure.get("context"), dict) else {"run_id": run.run_id}
            return {
                "type": str(failure.get("type") or run.status.value),
                "message": str(failure.get("message") or run.status.value),
                "context": SimulationLifecycleScheduler._flatten_pre_run_failure_context(context),
            }
        return {"type": run.status.value, "message": run.status.value, "context": {"run_id": run.run_id}}

    @staticmethod
    def _flatten_pre_run_failure_context(context: dict[str, Any]) -> dict[str, Any]:
        nested = context.get("context")
        if not isinstance(nested, dict):
            return context
        return {
            **nested,
            "pre_run_failure": context,
            "reason_code": str(context.get("reason_code") or nested.get("reason_code") or "PRE_RUN_FAILED"),
            "stage": str(context.get("stage") or "PRE_RUN_FAILED"),
            "failure_stage": context.get("failure_stage") or nested.get("failure_stage"),
            "binding_id": context.get("binding_id") or nested.get("binding_id"),
            "strategy_id": context.get("strategy_id") or nested.get("strategy_id"),
            "package_id": context.get("package_id") or nested.get("package_id"),
            "release_id": context.get("release_id") or nested.get("release_id"),
            "manifest_sha256": context.get("manifest_sha256") or nested.get("manifest_sha256"),
            "blocked_check": context.get("blocked_check") or nested.get("blocked_check"),
            "missing_relative_paths": context.get("missing_relative_paths") or nested.get("missing_relative_paths"),
            "trade_date": context.get("trade_date") or nested.get("trade_date"),
            "broker_backend": context.get("broker_backend"),
            "data_source": context.get("data_source"),
        }

    @staticmethod
    def _exception_context(exc: Exception) -> dict[str, Any]:
        raw_context = getattr(exc, "context", None)
        return _json_safe_preview(raw_context) if isinstance(raw_context, dict) else {}

    @staticmethod
    def _pre_run_failure_reason_code(exc: Exception, context: dict[str, Any]) -> str:
        reason_code = context.get("reason_code")
        if reason_code:
            return str(reason_code)
        blocked_context = SimulationLifecycleScheduler._pre_run_failure_blocked_check_context(context)
        if blocked_context and blocked_context.get("reason_code"):
            return str(blocked_context["reason_code"])
        return str(getattr(exc, "error_code", type(exc).__name__))

    @staticmethod
    def _pre_run_failure_stage(exc: Exception, context: dict[str, Any]) -> str:
        for key in ("failure_stage", "phase", "stage"):
            value = str(context.get(key) or "").strip().lower()
            if value in {
                "context",
                "selection",
                "selection_inference",
                "preflight",
                "validate",
                "validation",
                "build_plan",
                "planning",
                "binding_tick",
                "miniqmt_event_loop_submit",
                "miniqmt_reconcile_after_submit",
            }:
                return "validate" if value == "validation" else value
            if value in {"submit_preflight", "pre_submit"}:
                return "pre_submit"
        if isinstance(context.get("preflight"), dict) or context.get("blocked_check"):
            return "preflight"
        if type(exc).__name__ == "LiveInferencePreflightError":
            return "preflight"
        return "pre_run"

    @staticmethod
    def _pre_run_failure_blocked_check(context: dict[str, Any]) -> str | None:
        raw_blocked = context.get("blocked_check")
        if isinstance(raw_blocked, dict):
            name = raw_blocked.get("name")
            return str(name) if name else None
        if raw_blocked:
            return str(raw_blocked)
        check = SimulationLifecycleScheduler._pre_run_failure_blocked_check_payload(context)
        if not check:
            return None
        name = check.get("name")
        return str(name) if name else None

    @staticmethod
    def _pre_run_failure_blocked_check_context(context: dict[str, Any]) -> dict[str, Any]:
        raw_blocked = context.get("blocked_check")
        if isinstance(raw_blocked, dict):
            raw_context = raw_blocked.get("context")
            return dict(raw_context) if isinstance(raw_context, dict) else {}
        check = SimulationLifecycleScheduler._pre_run_failure_blocked_check_payload(context)
        if check:
            raw_context = check.get("context")
            return dict(raw_context) if isinstance(raw_context, dict) else {}
        return {}

    @staticmethod
    def _pre_run_failure_blocked_check_payload(context: dict[str, Any]) -> dict[str, Any] | None:
        preflight = context.get("preflight")
        checks = preflight.get("checks") if isinstance(preflight, dict) else None
        if not isinstance(checks, list):
            return None
        for check in checks:
            if isinstance(check, dict) and str(check.get("status") or "").upper() == "BLOCKED":
                return check
        return None

    @staticmethod
    def _pre_run_failure_missing_relative_paths(
        context: dict[str, Any],
        *,
        blocked_context: dict[str, Any] | None = None,
    ) -> list[str]:
        paths = SimulationLifecycleScheduler._string_list(context.get("missing_relative_paths"))
        if paths:
            return paths
        return SimulationLifecycleScheduler._string_list((blocked_context or {}).get("missing_relative_paths"))

    @staticmethod
    def _string_list(value: Any) -> list[str]:
        if isinstance(value, list | tuple | set):
            return [str(item) for item in value if str(item).strip()]
        if value is None:
            return []
        text = str(value).strip()
        return [text] if text else []

    def _clear_pre_run_failure_after_planning(
        self,
        build_result: SimulationPlanBuildResult,
    ) -> SimulationPlanBuildResult:
        if "pre_run_failure" not in build_result.run.run_payload_json:
            return build_result
        cleared = self.repository.update_simulation_daily_run(
            build_result.run.run_id,
            payload_unset=(
                "pre_run_failure",
                "pre_run_failure_last_observed_at",
                "pre_run_failure_observed_after_terminal",
                "submit_failure",
            ),
        )
        return replace(build_result, run=cleared)

    def post_close_reconcile_once(
        self,
        *,
        trade_date: date,
        data_source: str,
        limit: int = 100,
        strategy_id: str | None = None,
        as_of_time: datetime | None = None,
    ) -> SimulationSchedulerRunOnceResult:
        if limit <= 0:
            raise ValueError("limit must be positive")
        self._ensure_lifecycle_trading_day(trade_date=trade_date)
        if as_of_time is not None:
            as_of_time = self._scheduler_time(as_of_time)
        self._advance_miniqmt_quote_ingress_lifecycle()
        terminalized = self._terminalize_post_close_miniqmt_runs(
            trade_date=trade_date,
            broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
            strategy_id=strategy_id,
            limit=limit,
            as_of_time=as_of_time,
        )
        terminalized.extend(
            self._terminalize_post_close_localsim_runs(
                trade_date=trade_date,
                broker_backend=SimulationBrokerBackend.LOCAL_SIM,
                strategy_id=strategy_id,
                limit=limit,
                as_of_time=as_of_time,
            )
        )
        return SimulationSchedulerRunOnceResult(
            trade_date=trade_date,
            data_source=data_source,
            submit=False,
            total_bindings=0,
            results=(),
            stale_run_results=tuple(terminalized),
            as_of_time=as_of_time,
            schedule_windows=self._compute_schedule_windows(trade_date=trade_date, as_of_time=as_of_time),
        )

    def _ensure_lifecycle_trading_day(self, *, trade_date: date) -> None:
        service = self.trading_calendar_service
        if service is None:
            return
        try:
            status = self._lifecycle_trading_day_status(service=service, trade_date=trade_date)
        except DataUnavailableError as exc:
            context = getattr(exc, "context", None)
            if isinstance(context, dict) and context.get("reason_code"):
                raise
            raise DataUnavailableError(
                "simulation lifecycle scheduler trading-day gate could not load authoritative calendar status",
                context={
                    "reason_code": "SIMULATION_LIFECYCLE_TRADING_CALENDAR_UNAVAILABLE",
                    "trade_date": trade_date.isoformat(),
                    "service": type(service).__name__,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "error_context": context,
                },
            ) from exc
        except Exception as exc:
            raise DataUnavailableError(
                "simulation lifecycle scheduler trading-day gate failed",
                context={
                    "reason_code": "SIMULATION_LIFECYCLE_TRADING_CALENDAR_FAILED",
                    "trade_date": trade_date.isoformat(),
                    "service": type(service).__name__,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                },
            ) from exc
        if not bool(status.get("is_trading_day")):
            raise DataUnavailableError(
                "simulation lifecycle scheduler skipped non-trading day",
                context={
                    "reason_code": "SIMULATION_LIFECYCLE_NON_TRADING_DAY",
                    "trade_date": trade_date.isoformat(),
                    "next_trading_day": status.get("next_trading_day"),
                    "service": type(service).__name__,
                    "policy": "block_inner_run_once_roll_forward_plan_submit",
                },
            )

    def _lifecycle_trading_day_status(self, *, service: Any, trade_date: date) -> dict[str, Any]:
        status_method = getattr(service, "status", None)
        if callable(status_method):
            raw_status = dict(status_method(as_of_date=trade_date))
            if "is_trading_day" not in raw_status:
                raise DataUnavailableError(
                    "simulation lifecycle scheduler trading calendar status is missing is_trading_day",
                    context={
                        "reason_code": "SIMULATION_LIFECYCLE_TRADING_CALENDAR_STATUS_INVALID",
                        "trade_date": trade_date.isoformat(),
                        "service": type(service).__name__,
                        "status_keys": sorted(str(key) for key in raw_status),
                    },
                )
            is_trading_day = raw_status.get("is_trading_day")
            if not isinstance(is_trading_day, bool):
                raise DataUnavailableError(
                    "simulation lifecycle scheduler trading calendar status has non-boolean is_trading_day",
                    context={
                        "reason_code": "SIMULATION_LIFECYCLE_TRADING_CALENDAR_STATUS_INVALID",
                        "trade_date": trade_date.isoformat(),
                        "service": type(service).__name__,
                        "is_trading_day_type": type(is_trading_day).__name__,
                        "is_trading_day_value": repr(is_trading_day),
                    },
                )
            raw_status.setdefault("as_of_date", trade_date.isoformat())
            raw_status["is_trading_day"] = is_trading_day
            return raw_status
        is_trading_day_method = getattr(service, "is_trading_day", None)
        if callable(is_trading_day_method):
            is_trading_day = is_trading_day_method(trade_date)
            if not isinstance(is_trading_day, bool):
                raise DataUnavailableError(
                    "simulation lifecycle scheduler is_trading_day returned non-boolean value",
                    context={
                        "reason_code": "SIMULATION_LIFECYCLE_TRADING_CALENDAR_STATUS_INVALID",
                        "trade_date": trade_date.isoformat(),
                        "service": type(service).__name__,
                        "is_trading_day_type": type(is_trading_day).__name__,
                        "is_trading_day_value": repr(is_trading_day),
                    },
                )
            return {
                "as_of_date": trade_date.isoformat(),
                "is_trading_day": is_trading_day,
                "next_trading_day": None if is_trading_day else self._next_lifecycle_trading_day_iso(trade_date),
            }
        ensure_method = getattr(service, "ensure_trading_day", None)
        if callable(ensure_method):
            ensure_method(trade_date)
            return {"as_of_date": trade_date.isoformat(), "is_trading_day": True, "next_trading_day": None}
        raise DataUnavailableError(
            "simulation lifecycle scheduler trading calendar service lacks status/is_trading_day/ensure_trading_day",
            context={
                "reason_code": "SIMULATION_LIFECYCLE_TRADING_CALENDAR_METHOD_MISSING",
                "trade_date": trade_date.isoformat(),
                "service": type(service).__name__,
            },
        )

    def _next_lifecycle_trading_day_iso(self, trade_date: date) -> str | None:
        service = self.trading_calendar_service
        if service is None:
            return None
        for method_name in ("next_trading_day", "next_trading_day_after"):
            method = getattr(service, method_name, None)
            if callable(method):
                next_day = method(trade_date)
                return next_day.isoformat() if isinstance(next_day, date) else str(next_day)
        return None

    def _post_close_terminalized_binding_result(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        run_ids: set[str],
    ) -> SimulationSchedulerBindingResult | None:
        if not run_ids:
            return None
        existing = self.repository.get_simulation_daily_run_by_key(
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
            trade_date=trade_date,
        )
        if existing is None or existing.run_id not in run_ids:
            return None
        plan = self.repository.get_execution_plan(existing.execution_plan_id or "") if existing.execution_plan_id else None
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status="POST_CLOSE_TERMINALIZED",
            run=existing,
            execution_plan=plan,
            data_source=self._effective_market_data_source_for_binding(
                binding=binding,
                trade_date=trade_date,
                default_data_source=data_source,
            ),
        )

    def _terminalize_stale_miniqmt_active_runs(
        self,
        *,
        trade_date: date,
        broker_backend: SimulationBrokerBackend | str | None,
        strategy_id: str | None,
        limit: int,
        as_of_time: datetime | None,
        raise_on_error: bool = False,
    ) -> list[dict[str, Any]]:
        if broker_backend is not None and self._normalized_backend(broker_backend) != SimulationBrokerBackend.MINIQMT_SIM:
            return []
        terminalized: list[dict[str, Any]] = []
        seen_run_ids: set[str] = set()
        for status in _MINIQMT_STALE_ACTIVE_STATUSES:
            for run in self.repository.list_simulation_daily_runs(
                broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
                strategy_id=strategy_id,
                status=status,
                limit=limit,
            ):
                if run.run_id in seen_run_ids or run.trade_date >= trade_date:
                    continue
                seen_run_ids.add(run.run_id)
                terminalized_run = self._run_recovery_item_isolated(
                    stage="STALE_MINIQMT_TERMINALIZATION",
                    run=run,
                    raise_on_error=raise_on_error,
                    func=lambda run=run: self._terminalize_stale_miniqmt_run(
                        run=run,
                        scheduler_trade_date=trade_date,
                        as_of_time=as_of_time,
                    ),
                )
                if terminalized_run is not None:
                    terminalized.append(terminalized_run)
                if len(terminalized) >= limit:
                    return terminalized
        return terminalized[:limit]

    def _terminalize_stale_miniqmt_run(
        self,
        *,
        run: SimulationDailyRun,
        scheduler_trade_date: date,
        as_of_time: datetime | None,
    ) -> dict[str, Any]:
        had_side_effect = bool(run.run_payload_json.get("broker_called") or run.run_payload_json.get("qmt_batch_id"))
        if had_side_effect and self._mini_qmt_batch_has_broker_side_effect_evidence(run.run_payload_json):
            terminalized_run = self._post_close_terminalize_miniqmt_run(run=run, as_of_time=as_of_time)
            if terminalized_run is not None:
                terminalized_run.update(
                    {
                        "cross_day_terminalization": True,
                        "scheduler_trade_date": scheduler_trade_date.isoformat(),
                    }
                )
                return terminalized_run
        next_status = SimulationDailyRunStatus.FAILED_RETRYABLE if had_side_effect else SimulationDailyRunStatus.CANCELLED
        reason_code = (
            "MINIQMT_STALE_ACTIVE_WITH_BROKER_SIDE_EFFECT_UNRESOLVED"
            if had_side_effect
            else "MINIQMT_STALE_ACTIVE_WITHOUT_BROKER_SIDE_EFFECT"
        )
        evidence = {
            "schema_version": "miniqmt_stale_active_run_terminalization_v1",
            "reason": (
                "stale_historical_miniqmt_run_with_broker_side_effect_unresolved"
                if had_side_effect
                else "stale_historical_miniqmt_run_without_broker_side_effect"
            ),
            "reason_code": reason_code,
            "scheduler_trade_date": scheduler_trade_date.isoformat(),
            "stale_trade_date": run.trade_date.isoformat(),
            "previous_status": run.status.value,
            "had_broker_side_effect": had_side_effect,
            "broker_authoritative_terminalization_attempted": had_side_effect,
            "terminalized_at": (
                self._scheduler_time(as_of_time).isoformat()
                if as_of_time is not None
                else self._scheduler_now().isoformat()
            ),
        }
        updated = self.repository.update_simulation_daily_run(
            run.run_id,
            status=next_status,
            payload_patch={
                "last_stage": next_status.value,
                "stale_active_terminalization": evidence,
                "broker_called": bool(run.run_payload_json.get("broker_called")),
            },
        )
        return {
            "run_id": updated.run_id,
            "trade_date": updated.trade_date.isoformat(),
            "strategy_id": updated.strategy_id,
            "previous_status": run.status.value,
            "status": updated.status.value,
            "reason": evidence["reason"],
            "reason_code": reason_code,
        }

    def _terminalize_stale_localsim_active_runs(
        self,
        *,
        trade_date: date,
        broker_backend: SimulationBrokerBackend | str | None,
        strategy_id: str | None,
        limit: int,
        raise_on_error: bool = False,
    ) -> list[dict[str, Any]]:
        if broker_backend is not None and self._normalized_backend(broker_backend) != SimulationBrokerBackend.LOCAL_SIM:
            return []
        terminalized: list[dict[str, Any]] = []
        seen_run_ids: set[str] = set()
        for status in _LOCALSIM_STALE_ACTIVE_STATUSES:
            for run in self.repository.list_simulation_daily_runs(
                broker_backend=SimulationBrokerBackend.LOCAL_SIM,
                strategy_id=strategy_id,
                status=status,
                limit=limit,
            ):
                if run.run_id in seen_run_ids or run.trade_date >= trade_date:
                    continue
                seen_run_ids.add(run.run_id)
                terminalized_run = self._run_recovery_item_isolated(
                    stage="STALE_LOCALSIM_TERMINALIZATION",
                    run=run,
                    raise_on_error=raise_on_error,
                    func=lambda run=run: self._terminalize_stale_localsim_run(
                        run=run,
                        scheduler_trade_date=trade_date,
                    ),
                )
                if terminalized_run is not None:
                    terminalized.append(terminalized_run)
                if len(terminalized) >= limit:
                    return terminalized
        return terminalized[:limit]

    def _terminalize_stale_localsim_run(
        self,
        *,
        run: SimulationDailyRun,
        scheduler_trade_date: date,
    ) -> dict[str, Any]:
        had_side_effect = self._localsim_run_had_side_effect(run.run_payload_json)
        next_status = SimulationDailyRunStatus.FAILED_RETRYABLE if had_side_effect else SimulationDailyRunStatus.CANCELLED
        reason_code = (
            "LOCALSIM_STALE_ACTIVE_WITH_BROKER_SIDE_EFFECT"
            if had_side_effect
            else "LOCALSIM_STALE_ACTIVE_WITHOUT_BROKER_SIDE_EFFECT"
        )
        evidence = {
            "schema_version": "localsim_stale_active_run_terminalization_v1",
            "reason": (
                "stale_historical_localsim_run_with_broker_side_effect"
                if had_side_effect
                else "stale_historical_localsim_run_without_broker_side_effect"
            ),
            "reason_code": reason_code,
            "scheduler_trade_date": scheduler_trade_date.isoformat(),
            "stale_trade_date": run.trade_date.isoformat(),
            "previous_status": run.status.value,
            "terminal_status": next_status.value,
            "had_broker_side_effect": had_side_effect,
            "terminalized_at": self._scheduler_now().isoformat(),
        }
        updated = self.repository.update_simulation_daily_run(
            run.run_id,
            status=next_status,
            payload_patch={
                "last_stage": next_status.value,
                "localsim_stale_active_terminalization": evidence,
                "stale_active_terminalization": evidence,
                "broker_called": bool(run.run_payload_json.get("broker_called")),
            },
        )
        return {
            "run_id": updated.run_id,
            "trade_date": updated.trade_date.isoformat(),
            "strategy_id": updated.strategy_id,
            "broker_backend": updated.broker_backend.value,
            "previous_status": run.status.value,
            "status": updated.status.value,
            "reason": evidence["reason"],
            "reason_code": reason_code,
        }

    def _terminalize_post_close_miniqmt_runs(
        self,
        *,
        trade_date: date,
        broker_backend: SimulationBrokerBackend | str | None,
        strategy_id: str | None,
        limit: int,
        as_of_time: datetime | None,
        raise_on_error: bool = False,
    ) -> list[dict[str, Any]]:
        if not self._is_post_close_reconcile_time(as_of_time=as_of_time):
            return []
        if broker_backend is not None and self._normalized_backend(broker_backend) != SimulationBrokerBackend.MINIQMT_SIM:
            return []
        terminalized: list[dict[str, Any]] = []
        seen_run_ids: set[str] = set()
        for status in _MINIQMT_STALE_ACTIVE_STATUSES:
            for run in self.repository.list_simulation_daily_runs(
                broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
                strategy_id=strategy_id,
                status=status,
                limit=limit,
            ):
                if run.run_id in seen_run_ids or run.trade_date != trade_date:
                    continue
                seen_run_ids.add(run.run_id)
                terminalized_run = self._run_recovery_item_isolated(
                    stage="POST_CLOSE_MINIQMT_TERMINALIZATION",
                    run=run,
                    raise_on_error=raise_on_error,
                    func=lambda run=run: self._post_close_terminalize_miniqmt_run(
                        run=run,
                        as_of_time=as_of_time,
                    ),
                )
                if terminalized_run is None:
                    continue
                terminalized.append(terminalized_run)
                if len(terminalized) >= limit:
                    return terminalized
        return terminalized

    def _terminalize_post_close_localsim_runs(
        self,
        *,
        trade_date: date,
        broker_backend: SimulationBrokerBackend | str | None,
        strategy_id: str | None,
        limit: int,
        as_of_time: datetime | None,
        raise_on_error: bool = False,
    ) -> list[dict[str, Any]]:
        if not self._is_post_close_reconcile_time(as_of_time=as_of_time):
            return []
        if broker_backend is not None and self._normalized_backend(broker_backend) != SimulationBrokerBackend.LOCAL_SIM:
            return []
        terminalized: list[dict[str, Any]] = []
        seen_run_ids: set[str] = set()
        for status in _LOCALSIM_STALE_ACTIVE_STATUSES:
            for run in self.repository.list_simulation_daily_runs(
                broker_backend=SimulationBrokerBackend.LOCAL_SIM,
                strategy_id=strategy_id,
                status=status,
                limit=limit,
            ):
                if run.run_id in seen_run_ids or run.trade_date != trade_date:
                    continue
                seen_run_ids.add(run.run_id)
                terminalized_run = self._run_recovery_item_isolated(
                    stage="POST_CLOSE_LOCALSIM_TERMINALIZATION",
                    run=run,
                    raise_on_error=raise_on_error,
                    func=lambda run=run: self._post_close_terminalize_localsim_run(
                        run=run,
                        as_of_time=as_of_time,
                    ),
                )
                if terminalized_run is None:
                    continue
                terminalized.append(terminalized_run)
                if len(terminalized) >= limit:
                    return terminalized
        return terminalized

    def _post_close_terminalize_localsim_run(
        self,
        *,
        run: SimulationDailyRun,
        as_of_time: datetime | None,
    ) -> dict[str, Any] | None:
        terminal_status, reason, reason_code, audit_state = self._localsim_post_close_terminal_status(run)
        if terminal_status is None:
            return None
        payload = run.run_payload_json
        evidence = {
            "schema_version": "localsim_post_close_terminalization_v1",
            "reason": reason,
            "reason_code": reason_code,
            "previous_status": run.status.value,
            "terminal_status": terminal_status.value,
            "trade_date": run.trade_date.isoformat(),
            "as_of_time": self._scheduler_time(as_of_time).isoformat() if as_of_time is not None else None,
            "broker_called": bool(payload.get("broker_called")),
            "submitted_intents": int(payload.get("submitted_intents") or 0),
            "failed_intents": int(payload.get("failed_intents") or 0),
            "local_sim_persistence_status": (
                payload.get("local_sim_persistence", {}).get("status")
                if isinstance(payload.get("local_sim_persistence"), dict)
                else None
            ),
            "local_sim_cash_fit_status": (
                payload.get("local_sim_cash_fit", {}).get("status")
                if isinstance(payload.get("local_sim_cash_fit"), dict)
                else None
            ),
            "had_broker_side_effect": self._localsim_run_had_side_effect(payload),
            "audit_state": audit_state,
        }
        updated = self.repository.update_simulation_daily_run(
            run.run_id,
            status=terminal_status,
            payload_patch={
                "last_stage": terminal_status.value,
                "localsim_post_close_terminalization": evidence,
            },
            payload_unset=("submit_failure", "local_sim_retry_diagnostics")
            if terminal_status == SimulationDailyRunStatus.SUCCEEDED
            else None,
        )
        return {
            "run_id": updated.run_id,
            "trade_date": updated.trade_date.isoformat(),
            "strategy_id": updated.strategy_id,
            "broker_backend": updated.broker_backend.value,
            "previous_status": run.status.value,
            "status": updated.status.value,
            "reason": reason,
            "reason_code": reason_code,
            "post_close_terminalization": True,
        }

    @staticmethod
    def _localsim_post_close_terminal_status(
        run: SimulationDailyRun,
    ) -> tuple[SimulationDailyRunStatus | None, str | None, str | None, str | None]:
        payload = run.run_payload_json
        persistence = payload.get("local_sim_persistence") if isinstance(payload.get("local_sim_persistence"), dict) else {}
        persistence_status = str(persistence.get("status") or "").upper()
        if persistence_status == "PERSISTED":
            return (
                SimulationDailyRunStatus.SUCCEEDED,
                "localsim_post_close_persisted_success",
                "LOCALSIM_POST_CLOSE_PERSISTED_SUCCESS",
                "succeeded_after_close",
            )
        if persistence_status == "PERSISTED_WITH_CAPACITY_RESIDUAL":
            return (
                SimulationDailyRunStatus.FAILED_TERMINAL,
                "localsim_post_close_capacity_residual_terminal_failed",
                "LOCALSIM_POST_CLOSE_CAPACITY_RESIDUAL_TERMINAL",
                "failed_terminal_after_close",
            )
        if bool(payload.get("no_rebalance_required")) and not bool(payload.get("broker_called")):
            return (
                SimulationDailyRunStatus.SUCCEEDED,
                "localsim_post_close_no_rebalance_success",
                "LOCALSIM_POST_CLOSE_NO_REBALANCE_SUCCESS",
                "succeeded_no_rebalance_after_close",
            )
        if SimulationLifecycleScheduler._localsim_run_had_side_effect(payload):
            return (
                SimulationDailyRunStatus.FAILED_TERMINAL,
                "localsim_post_close_missing_durable_persistence_terminal_failed",
                "LOCALSIM_POST_CLOSE_DURABLE_PERSISTENCE_MISSING",
                "failed_terminal_after_close",
            )
        return (
            SimulationDailyRunStatus.CANCELLED,
            "localsim_post_close_no_broker_side_effect_cancelled",
            "LOCALSIM_POST_CLOSE_NO_BROKER_SIDE_EFFECT",
            "cancelled_after_close",
        )

    @staticmethod
    def _localsim_run_had_side_effect(payload: dict[str, Any]) -> bool:
        if bool(payload.get("broker_called")):
            return True
        if isinstance(payload.get("local_sim_persistence"), dict):
            return True
        submitted_raw = payload.get("submitted_intents")
        if submitted_raw is None:
            return False
        try:
            submitted = int(submitted_raw)
        except (TypeError, ValueError):
            logger.warning(
                "LocalSim terminalization found invalid submitted_intents; treating run as side-effect-bearing",
                extra={
                    "reason_code": "LOCALSIM_TERMINALIZATION_SUBMITTED_INTENTS_INVALID",
                    "payload_run_id": payload.get("run_id"),
                    "payload_strategy_id": payload.get("strategy_id"),
                    "submitted_intents": submitted_raw,
                    "submitted_intents_type": type(submitted_raw).__name__,
                    "policy": "fail_closed_side_effect_assumed",
                },
            )
            return True
        if submitted < 0:
            logger.warning(
                "LocalSim terminalization found negative submitted_intents; treating run as side-effect-bearing",
                extra={
                    "reason_code": "LOCALSIM_TERMINALIZATION_SUBMITTED_INTENTS_NEGATIVE",
                    "payload_run_id": payload.get("run_id"),
                    "payload_strategy_id": payload.get("strategy_id"),
                    "submitted_intents": submitted,
                    "policy": "fail_closed_side_effect_assumed",
                },
            )
            return True
        return submitted > 0

    def _post_close_terminalize_miniqmt_run(
        self,
        *,
        run: SimulationDailyRun,
        as_of_time: datetime | None,
    ) -> dict[str, Any] | None:
        payload = run.run_payload_json
        if not self._mini_qmt_batch_has_broker_side_effect_evidence(payload):
            return None
        previous_open_order_evidence = self._latest_miniqmt_payload_evidence(payload, "open_order_evidence")
        previous_submit_result_gate = self._latest_miniqmt_payload_evidence(payload, "submit_result_gate")
        fresh_payload, fresh_reconcile = self._fresh_miniqmt_post_close_payload(
            run=run,
            as_of_time=as_of_time,
        )
        terminal_status, reason = self._miniqmt_post_close_terminal_status(fresh_payload)
        event_loop_pending_after_close = self._miniqmt_event_loop_pending_after_close(fresh_payload)
        if terminal_status is None and event_loop_pending_after_close:
            terminal_status = SimulationDailyRunStatus.FAILED_RETRYABLE
            reason = "miniqmt_post_close_event_loop_pending_algos_untriggered"
        if terminal_status is None:
            return None
        summary = self._mini_qmt_batch_residual_summary(fresh_payload)
        capacity_residual_observability = self._miniqmt_capacity_residual_observability(
            fresh_payload,
            reason=reason,
            source="post_close_terminalization",
        )
        evidence = {
            "schema_version": "miniqmt_post_close_terminalization_v1",
            "reason": reason,
            "previous_status": run.status.value,
            "terminal_status": terminal_status.value,
            "trade_date": run.trade_date.isoformat(),
            "as_of_time": self._scheduler_time(as_of_time).isoformat() if as_of_time is not None else None,
            "qmt_batch_status": fresh_payload.get("qmt_batch_status"),
            "qmt_batch_id": fresh_payload.get("qmt_batch_id"),
            "residual_summary": summary,
            "fresh_reconcile": fresh_reconcile,
            "previous_open_order_evidence": previous_open_order_evidence,
            "previous_submit_result_gate": previous_submit_result_gate,
            "open_order_evidence": self._latest_miniqmt_payload_evidence(fresh_payload, "open_order_evidence"),
            "submit_result_gate": self._latest_miniqmt_payload_evidence(fresh_payload, "submit_result_gate"),
            "audit_state": self._miniqmt_post_close_audit_state(terminal_status, reason),
        }
        if event_loop_pending_after_close:
            evidence["event_loop_pending_after_close"] = event_loop_pending_after_close
        if capacity_residual_observability:
            evidence["miniqmt_capacity_residual_observability"] = capacity_residual_observability
        payload_patch = {
            "last_stage": terminal_status.value,
            "miniqmt_post_close_terminalization": evidence,
        }
        payload_patch.update(self._miniqmt_capacity_residual_payload_patch(capacity_residual_observability))
        updated = self.repository.update_simulation_daily_run(
            run.run_id,
            status=terminal_status,
            payload_patch=payload_patch,
            payload_unset=("submit_failure",) if terminal_status == SimulationDailyRunStatus.SUCCEEDED else None,
        )
        return {
            "run_id": updated.run_id,
            "trade_date": updated.trade_date.isoformat(),
            "strategy_id": updated.strategy_id,
            "previous_status": run.status.value,
            "status": updated.status.value,
            "reason": reason,
            "post_close_terminalization": True,
            **self._miniqmt_capacity_residual_result_fields(updated),
        }

    def _fresh_miniqmt_post_close_payload(
        self,
        *,
        run: SimulationDailyRun,
        as_of_time: datetime | None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        try:
            binding = self.repository.get_simulation_release_binding(run.binding_id)
            runtime_release = self.repository.get_strategy_runtime_release(run.release_id)
            context = self.context_provider.load_context(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=run.trade_date,
            )
            reconciliation = self._reconcile_after_submit_with_timeout(binding=binding, run=run, context=context)
        except Exception as exc:
            if isinstance(exc, DataUnavailableError) and getattr(exc, "context", {}).get(
                "reason_code"
            ) == "MINIQMT_POST_CLOSE_FRESH_RECONCILE_FAILED":
                raise
            raise DataUnavailableError(
                "MiniQMT post-close terminalization requires a fresh broker reconcile before terminal status",
                context={
                    "reason_code": "MINIQMT_POST_CLOSE_FRESH_RECONCILE_FAILED",
                    "run_id": run.run_id,
                    "binding_id": run.binding_id,
                    "strategy_id": run.strategy_id,
                    "release_id": run.release_id,
                    "trade_date": run.trade_date.isoformat(),
                    "as_of_time": as_of_time.isoformat() if as_of_time is not None else None,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "error_context": getattr(exc, "context", None),
                },
            ) from exc
        if reconciliation is None:
            raise DataUnavailableError(
                "MiniQMT post-close terminalization fresh broker reconcile returned no payload",
                context={
                    "reason_code": "MINIQMT_POST_CLOSE_FRESH_RECONCILE_MISSING",
                    "run_id": run.run_id,
                    "binding_id": run.binding_id,
                    "strategy_id": run.strategy_id,
                    "release_id": run.release_id,
                    "trade_date": run.trade_date.isoformat(),
                    "as_of_time": as_of_time.isoformat() if as_of_time is not None else None,
                },
            )
        refreshed_run = self.repository.get_simulation_daily_run(run.run_id)
        payload = refreshed_run.run_payload_json
        sync_evidence = payload.get("sync_after_submit") if isinstance(payload.get("sync_after_submit"), dict) else {}
        return payload, {
            "schema_version": "miniqmt_post_close_fresh_reconcile_v1",
            "source": "qmt_broker_snapshot_and_strategy_ledger",
            "as_of_time": as_of_time.isoformat() if as_of_time is not None else None,
            "run_id": run.run_id,
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "account_id": binding.broker_account_id,
            "reconcile_payload_key": "reconcile_after_submit",
            "sync_payload_key": "sync_after_submit",
            "sync_evidence": sync_evidence,
            "open_order_evidence": self._latest_miniqmt_payload_evidence(payload, "open_order_evidence"),
            "submit_result_gate": self._latest_miniqmt_payload_evidence(payload, "submit_result_gate"),
        }

    @staticmethod
    def _is_post_close_reconcile_time(*, as_of_time: datetime | None) -> bool:
        if as_of_time is None:
            return False
        local_as_of = SimulationLifecycleScheduler._scheduler_time(as_of_time)
        return local_as_of.time().replace(second=0, microsecond=0) >= _POST_CLOSE_RECONCILE_TIME

    @staticmethod
    def _miniqmt_post_close_terminal_status(
        payload: dict[str, Any],
    ) -> tuple[SimulationDailyRunStatus | None, str | None]:
        open_order_evidence = SimulationLifecycleScheduler._latest_miniqmt_payload_evidence(
            payload,
            "open_order_evidence",
        )
        open_order_count = int(open_order_evidence.get("open_order_count") or 0) if open_order_evidence else 0
        if open_order_count > 0:
            return SimulationDailyRunStatus.FAILED_TERMINAL, "miniqmt_post_close_open_orders_terminal_failed"
        if SimulationLifecycleScheduler._mini_qmt_batch_succeeded(payload):
            return SimulationDailyRunStatus.SUCCEEDED, "miniqmt_post_close_batch_succeeded"
        if SimulationLifecycleScheduler._mini_qmt_batch_has_terminal_capacity_residual(payload):
            return SimulationDailyRunStatus.SUCCEEDED, "miniqmt_post_close_capacity_residual_skipped"
        if SimulationLifecycleScheduler._mini_qmt_batch_has_retryable_buy_residual(payload):
            return SimulationDailyRunStatus.FAILED_RETRYABLE, "miniqmt_post_close_buy_residual_unresolved"
        return None, None

    @staticmethod
    def _miniqmt_post_close_audit_state(status: SimulationDailyRunStatus, reason: str | None) -> str:
        if status == SimulationDailyRunStatus.SUCCEEDED:
            if reason == "miniqmt_post_close_capacity_residual_skipped":
                return "succeeded_with_capacity_residual"
            return "succeeded_after_close"
        if status == SimulationDailyRunStatus.FAILED_RETRYABLE:
            return "failed_retryable_after_close"
        return "failed_terminal_after_close"

    @staticmethod
    def _miniqmt_event_loop_pending_after_close(payload: dict[str, Any]) -> dict[str, Any] | None:
        if not SimulationLifecycleScheduler._mini_qmt_event_loop_has_pending_algos(payload):
            return None
        driver = payload.get("miniqmt_event_loop_tick_driver") if isinstance(payload.get("miniqmt_event_loop_tick_driver"), dict) else {}
        pending_parent_ids = driver.get("pending_parent_intent_ids") if isinstance(driver.get("pending_parent_intent_ids"), list) else []
        return {
            "schema_version": "miniqmt_event_loop_pending_after_close_v1",
            "reason_code": "MINIQMT_EVENT_LOOP_PENDING_ALGOS_MARKET_CLOSED",
            "stage": "MINIQMT_POST_CLOSE_TERMINALIZATION",
            "reason": "event_loop_algorithms_remained_running_without_child_order_until_market_close",
            "pending_intents": payload.get("pending_intents"),
            "pending_parent_intent_ids": list(pending_parent_ids),
            "qmt_batch_id": payload.get("qmt_batch_id"),
            "qmt_batch_status": payload.get("qmt_batch_status"),
        }

    @staticmethod
    def _latest_miniqmt_payload_evidence(payload: dict[str, Any], key: str) -> dict[str, Any]:
        for container_key in ("reconcile_after_submit", "sync_after_submit", "sync_before_submit"):
            container = payload.get(container_key)
            if not isinstance(container, dict):
                continue
            evidence = container.get(key)
            if isinstance(evidence, dict):
                return evidence
        return {}

    def _partition_retired_package_bindings(
        self,
        *,
        bindings: list[SimulationReleaseBinding],
        data_source: str,
        package_status_cache: dict[str, PackageStatus],
    ) -> tuple[
        list[SimulationReleaseBinding],
        list[SimulationSchedulerBindingResult],
        set[tuple[str, SimulationBrokerBackend]],
    ]:
        eligible: list[SimulationReleaseBinding] = []
        skipped: list[SimulationSchedulerBindingResult] = []
        blocked_keys: set[tuple[str, SimulationBrokerBackend]] = set()
        for binding in bindings:
            status = self._package_lifecycle_status(
                package_id=binding.package_id,
                package_status_cache=package_status_cache,
            )
            if status != PackageStatus.RETIRED:
                eligible.append(binding)
                continue
            blocked_keys.add((binding.strategy_id, binding.broker_backend))
            skipped.append(self._retired_package_binding_result(binding=binding, data_source=data_source))
        return eligible, skipped, blocked_keys

    def _package_lifecycle_status(
        self,
        *,
        package_id: str,
        package_status_cache: dict[str, PackageStatus],
    ) -> PackageStatus:
        cached = package_status_cache.get(package_id)
        if cached is not None:
            return cached
        package_repository = getattr(self.selection_service, "package_repository", None)
        get_package = getattr(package_repository, "get", None)
        if not callable(get_package):
            raise RuntimeConfigInvalidError(
                "simulation scheduler requires the authoritative StrategyPackage lifecycle reader",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_PACKAGE_LIFECYCLE_READER_MISSING",
                    "package_id": package_id,
                },
            )
        record = get_package(package_id)
        record_package_id = str(getattr(record, "package_id", "") or "").strip()
        raw_status = getattr(record, "package_status", None)
        status_value = str(getattr(raw_status, "value", raw_status) or "").strip()
        try:
            status = PackageStatus(status_value)
        except ValueError as exc:
            raise RuntimeConfigInvalidError(
                "simulation scheduler StrategyPackage lifecycle status is invalid",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_PACKAGE_LIFECYCLE_INVALID",
                    "package_id": package_id,
                    "record_package_id": record_package_id or None,
                    "package_status": status_value or None,
                },
            ) from exc
        if record_package_id != package_id:
            raise RuntimeConfigInvalidError(
                "simulation scheduler StrategyPackage lifecycle identity does not match the binding",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_PACKAGE_LIFECYCLE_IDENTITY_MISMATCH",
                    "package_id": package_id,
                    "record_package_id": record_package_id or None,
                    "package_status": status.value,
                },
            )
        package_status_cache[package_id] = status
        return status

    @staticmethod
    def _retired_package_binding_result(
        *,
        binding: SimulationReleaseBinding,
        data_source: str,
    ) -> SimulationSchedulerBindingResult:
        diagnostic = {
            "schema_version": "simulation_package_lifecycle_skip_v1",
            "reason_code": "SIMULATION_BINDING_PACKAGE_RETIRED",
            "stage": "BINDING_SELECTION",
            "action": "SKIP",
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "package_id": binding.package_id,
            "package_status": PackageStatus.RETIRED.value,
            "broker_backend": binding.broker_backend.value,
            "broker_called": False,
            "strategy_package_revalidation_performed": False,
        }
        logger.warning("Simulation binding skipped because StrategyPackage is retired: %s", diagnostic)
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status="SKIPPED_RETIRED_PACKAGE",
            lifecycle_diagnostic=diagnostic,
            data_source=data_source,
        )

    def _with_unattended_roll_forward_bindings(
        self,
        *,
        bindings: list[SimulationReleaseBinding],
        trade_date: date,
        limit: int,
        broker_backend: SimulationBrokerBackend | str | None,
        strategy_id: str | None,
        release_id: str | None,
        approval_states: tuple[SimulationBindingApprovalState, ...] | None,
        data_source: str,
        package_status_cache: dict[str, PackageStatus],
        lifecycle_skips: list[SimulationSchedulerBindingResult],
        blocked_binding_keys: set[tuple[str, SimulationBrokerBackend]],
    ) -> list[SimulationReleaseBinding]:
        if release_id is not None:
            return bindings

        bindings = self._without_superseded_unattended_bindings(bindings)
        bindings = [
            self._rebase_unattended_binding_to_authoritative_manifest(
                binding=binding,
                trade_date=trade_date,
            )
            for binding in bindings
        ]

        remaining_slots = limit - len(bindings)
        if remaining_slots <= 0:
            return bindings
        existing_keys = {(item.strategy_id, item.broker_backend) for item in bindings}
        existing_keys.update(blocked_binding_keys)
        roll_forwarded: list[SimulationReleaseBinding] = []
        for backend in self._roll_forward_backends_for_filter(broker_backend):
            if len(roll_forwarded) >= remaining_slots:
                break
            source_candidates = self.repository.list_latest_simulation_release_bindings(
                strategy_id=strategy_id,
                broker_backend=backend,
                approval_states=approval_states,
                effective_from_on_or_before=trade_date,
                limit=limit + remaining_slots,
            )
            for source in source_candidates:
                if (source.strategy_id, source.broker_backend) in existing_keys:
                    continue
                package_status = self._package_lifecycle_status(
                    package_id=source.package_id,
                    package_status_cache=package_status_cache,
                )
                if package_status == PackageStatus.RETIRED:
                    lifecycle_skips.append(
                        self._retired_package_binding_result(binding=source, data_source=data_source)
                    )
                    existing_keys.add((source.strategy_id, source.broker_backend))
                    continue
                if not self._binding_can_roll_forward(source=source, trade_date=trade_date):
                    continue
                roll_forwarded.append(self._roll_forward_unattended_binding(source=source, trade_date=trade_date))
                existing_keys.add((source.strategy_id, source.broker_backend))
                if len(roll_forwarded) >= remaining_slots:
                    break

        if not roll_forwarded:
            return bindings
        combined = [*bindings, *roll_forwarded]
        combined.sort(key=lambda item: (item.created_at, item.binding_id), reverse=True)
        return combined[:limit]

    @staticmethod
    def _without_superseded_unattended_bindings(
        bindings: list[SimulationReleaseBinding],
    ) -> list[SimulationReleaseBinding]:
        """Hide only explicitly superseded same-day roll-forward bindings.

        A manifest rebase creates a new immutable release/binding instead of
        rewriting the already persisted identity. Both rows can be active on
        the rebase day, so the successor's explicit lineage is the only safe
        basis for suppressing the old row. Unrelated overlapping bindings are
        deliberately left visible and continue to fail through their normal
        lifecycle checks.
        """

        superseded_ids: set[str] = set()
        for binding in bindings:
            config = binding.binding_config_json if isinstance(binding.binding_config_json, dict) else {}
            metadata = config.get("metadata") if isinstance(config.get("metadata"), dict) else {}
            if metadata.get("manifest_identity_source") != "strategy_package_current_manifest":
                continue
            source_binding_id = str(metadata.get("extends_binding_id") or "").strip()
            if source_binding_id:
                superseded_ids.add(source_binding_id)
        return [binding for binding in bindings if binding.binding_id not in superseded_ids]

    def _rebase_unattended_binding_to_authoritative_manifest(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> SimulationReleaseBinding:
        config = binding.binding_config_json if isinstance(binding.binding_config_json, dict) else {}
        metadata = config.get("metadata") if isinstance(config.get("metadata"), dict) else {}
        if metadata.get("purpose") not in {
            "localsim_unattended_daily_roll_forward",
            "miniqmt_unattended_daily_roll_forward",
        }:
            return binding

        source_release = self.repository.get_strategy_runtime_release(binding.release_id)
        authoritative_manifest_sha256 = self._authoritative_package_manifest_sha256(
            package_id=binding.package_id,
        )
        if (
            binding.manifest_sha256 == authoritative_manifest_sha256
            and source_release.manifest_sha256 == authoritative_manifest_sha256
        ):
            return binding

        existing_run = self.repository.get_simulation_daily_run_by_key(
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
            trade_date=trade_date,
        )
        if existing_run is not None and self._run_has_broker_side_effect_evidence(existing_run):
            # Never move an already submitted run to a different immutable
            # package identity. Returning the old binding preserves the loud
            # downstream identity conflict for operator diagnostics.
            return binding

        return self._roll_forward_unattended_binding(
            source=binding,
            trade_date=trade_date,
            authoritative_manifest_sha256=authoritative_manifest_sha256,
        )

    @staticmethod
    def _run_has_broker_side_effect_evidence(run: SimulationDailyRun) -> bool:
        payload = run.run_payload_json if isinstance(run.run_payload_json, dict) else {}
        if bool(payload.get("broker_called")):
            return True
        raw_submitted = payload.get("submitted_intents")
        if raw_submitted is not None:
            try:
                if int(raw_submitted) > 0:
                    return True
            except (TypeError, ValueError):
                return True
        return SimulationLifecycleScheduler._mini_qmt_batch_has_broker_side_effect_evidence(payload)

    def _authoritative_package_manifest_sha256(self, *, package_id: str) -> str:
        package_repository = getattr(self.selection_service, "package_repository", None)
        get_package = getattr(package_repository, "get", None)
        if not callable(get_package):
            raise RuntimeConfigInvalidError(
                "unattended simulation roll-forward requires the authoritative StrategyPackage repository",
                context={
                    "reason_code": "SIMULATION_ROLL_FORWARD_PACKAGE_IDENTITY_SOURCE_MISSING",
                    "package_id": package_id,
                },
            )
        record = get_package(package_id)
        record_package_id = str(getattr(record, "package_id", "") or "").strip()
        manifest_sha256 = str(getattr(record, "manifest_sha256", "") or "").strip()
        if record_package_id != package_id or not manifest_sha256:
            raise RuntimeConfigInvalidError(
                "authoritative StrategyPackage identity is incomplete for unattended roll-forward",
                context={
                    "reason_code": "SIMULATION_ROLL_FORWARD_PACKAGE_IDENTITY_INVALID",
                    "package_id": package_id,
                    "record_package_id": record_package_id or None,
                    "manifest_sha256": manifest_sha256 or None,
                },
            )
        return manifest_sha256

    @staticmethod
    def _normalized_backend(value: SimulationBrokerBackend | str) -> SimulationBrokerBackend:
        return value if isinstance(value, SimulationBrokerBackend) else SimulationBrokerBackend(str(value))

    @staticmethod
    def _roll_forward_backends_for_filter(
        broker_backend: SimulationBrokerBackend | str | None,
    ) -> tuple[SimulationBrokerBackend, ...]:
        if broker_backend is not None:
            return (SimulationLifecycleScheduler._normalized_backend(broker_backend),)
        return (SimulationBrokerBackend.LOCAL_SIM, SimulationBrokerBackend.MINIQMT_SIM)

    @staticmethod
    def _binding_can_roll_forward(*, source: SimulationReleaseBinding, trade_date: date) -> bool:
        if source.broker_backend not in {SimulationBrokerBackend.LOCAL_SIM, SimulationBrokerBackend.MINIQMT_SIM}:
            return False
        if source.effective_from is not None and source.effective_from > trade_date:
            return False
        if source.effective_to is None or source.effective_to >= trade_date:
            return False
        config = source.binding_config_json or {}
        metadata = config.get("metadata") if isinstance(config, dict) else None
        if isinstance(metadata, dict) and metadata.get("disable_unattended_roll_forward") is True:
            return False
        return True

    def _roll_forward_unattended_binding(
        self,
        *,
        source: SimulationReleaseBinding,
        trade_date: date,
        authoritative_manifest_sha256: str | None = None,
    ) -> SimulationReleaseBinding:
        source_release = self.repository.get_strategy_runtime_release(source.release_id)
        resolved_manifest_sha256 = authoritative_manifest_sha256 or self._authoritative_package_manifest_sha256(
            package_id=source_release.package_id,
        )
        release_service = StrategyRuntimeReleaseService(repository=self.repository)
        created_by = self._roll_forward_created_by(source.broker_backend)
        release_metadata = self._roll_forward_release_metadata(
            source_release=source_release,
            source_binding=source,
            trade_date=trade_date,
            authoritative_manifest_sha256=resolved_manifest_sha256,
        )
        validation_evidence = self._roll_forward_validation_evidence(
            source_release=source_release,
            source_binding=source,
            trade_date=trade_date,
            authoritative_manifest_sha256=resolved_manifest_sha256,
        )
        new_release = release_service.create_release(
            package_id=source_release.package_id,
            manifest_sha256=resolved_manifest_sha256,
            runtime_profile_id=source_release.runtime_profile_id,
            runtime_profile_version_id=source_release.runtime_profile_version_id,
            runtime_profile_sha256=source_release.runtime_profile_sha256,
            daily_strategy_profile_version_id=source_release.daily_strategy_profile_version_id,
            execution_policy_version_id=source_release.execution_policy_version_id,
            execution_policy_sha256=source_release.execution_policy_sha256,
            tail_policy_version_id=source_release.tail_policy_version_id,
            tail_policy_sha256=source_release.tail_policy_sha256,
            execution_policy_json=self._release_execution_policy_json(source_release),
            base_release_id=source_release.release_id,
            validation_state=source_release.validation_state,
            validation_evidence=validation_evidence,
            release_metadata=release_metadata,
            effective_from=trade_date,
            effective_to=trade_date,
            created_by=created_by,
            created_reason=(
                f"Auto roll-forward {source.broker_backend.value} runtime release for unattended daily simulation "
                f"on {trade_date.isoformat()}."
            ),
        )
        binding_metadata = self._roll_forward_binding_metadata(
            source_release=source_release,
            source_binding=source,
            new_release=new_release,
            trade_date=trade_date,
            authoritative_manifest_sha256=resolved_manifest_sha256,
        )
        return release_service.create_binding(
            strategy_id=source.strategy_id,
            release=new_release,
            broker_backend=source.broker_backend,
            capital_allocation=float(source.capital_allocation),
            broker_account_id=source.broker_account_id,
            account_group_id=source.account_group_id,
            strategy_slot_id=source.strategy_slot_id,
            strategy_name=self._strategy_name_for_roll_forward(source=source, trade_date=trade_date),
            order_remark_prefix=source.order_remark_prefix,
            approval_state=source.approval_state,
            binding_metadata=binding_metadata,
            miniqmt_quote_control=(
                dict(source.binding_config_json["miniqmt_quote_control"])
                if isinstance(source.binding_config_json.get("miniqmt_quote_control"), dict)
                else None
            ),
            effective_from=trade_date,
            effective_to=trade_date,
            created_by=created_by,
            created_reason=(
                f"Auto roll-forward {source.broker_backend.value} binding for unattended daily simulation "
                f"on {trade_date.isoformat()}."
            ),
        )

    @staticmethod
    def _roll_forward_created_by(backend: SimulationBrokerBackend) -> str:
        if backend == SimulationBrokerBackend.MINIQMT_SIM:
            return _MINIQMT_ROLL_FORWARD_CREATED_BY
        return _LOCALSIM_ROLL_FORWARD_CREATED_BY

    @staticmethod
    def _roll_forward_purpose(backend: SimulationBrokerBackend) -> str:
        if backend == SimulationBrokerBackend.MINIQMT_SIM:
            return "miniqmt_unattended_daily_roll_forward"
        return "localsim_unattended_daily_roll_forward"

    @staticmethod
    def _release_execution_policy_json(release: StrategyRuntimeRelease) -> dict[str, Any] | None:
        config = release.release_config_json or {}
        execution_policy = config.get("execution_policy") if isinstance(config, dict) else None
        policy_json = execution_policy.get("policy_json") if isinstance(execution_policy, dict) else None
        return deepcopy(policy_json) if isinstance(policy_json, dict) and policy_json else None

    @staticmethod
    def _roll_forward_release_metadata(
        *,
        source_release: StrategyRuntimeRelease,
        source_binding: SimulationReleaseBinding,
        trade_date: date,
        authoritative_manifest_sha256: str,
    ) -> dict[str, Any]:
        config = source_release.release_config_json or {}
        metadata = deepcopy(config.get("metadata") if isinstance(config, dict) else {}) or {}
        created_by = SimulationLifecycleScheduler._roll_forward_created_by(source_binding.broker_backend)
        purpose = SimulationLifecycleScheduler._roll_forward_purpose(source_binding.broker_backend)
        metadata.update(
            {
                "source": created_by,
                "purpose": purpose,
                "target_trade_date": trade_date.isoformat(),
                "extends_release_id": source_release.release_id,
                "extends_binding_id": source_binding.binding_id,
                "manifest_identity_source": "strategy_package_current_manifest",
                "source_release_manifest_sha256": source_release.manifest_sha256,
                "authoritative_manifest_sha256": authoritative_manifest_sha256,
                "manifest_identity_changed": source_release.manifest_sha256 != authoritative_manifest_sha256,
                "roll_forward_policy": {
                    "schema_version": "localsim_roll_forward_policy_v1",
                    "immutable_daily_release": True,
                    "no_manual_db_write_required": True,
                },
            }
        )
        return metadata

    @staticmethod
    def _roll_forward_validation_evidence(
        *,
        source_release: StrategyRuntimeRelease,
        source_binding: SimulationReleaseBinding,
        trade_date: date,
        authoritative_manifest_sha256: str,
    ) -> dict[str, Any]:
        evidence = deepcopy(source_release.validation_evidence or {})
        created_by = SimulationLifecycleScheduler._roll_forward_created_by(source_binding.broker_backend)
        evidence.update(
            {
                "source": created_by,
                "purpose": f"{source_binding.broker_backend.value} unattended daily roll-forward",
                "target_trade_date": trade_date.isoformat(),
                "extends_release_id": source_release.release_id,
                "extends_binding_id": source_binding.binding_id,
                "manifest_identity": {
                    "source": "strategy_package_current_manifest",
                    "source_release_manifest_sha256": source_release.manifest_sha256,
                    "authoritative_manifest_sha256": authoritative_manifest_sha256,
                    "identity_changed": source_release.manifest_sha256 != authoritative_manifest_sha256,
                    "strategy_package_revalidation_performed": False,
                },
            }
        )
        return evidence

    @staticmethod
    def _roll_forward_binding_metadata(
        *,
        source_release: StrategyRuntimeRelease,
        source_binding: SimulationReleaseBinding,
        new_release: StrategyRuntimeRelease,
        trade_date: date,
        authoritative_manifest_sha256: str,
    ) -> dict[str, Any]:
        config = source_binding.binding_config_json or {}
        metadata = deepcopy(config.get("metadata") if isinstance(config, dict) else {}) or {}
        created_by = SimulationLifecycleScheduler._roll_forward_created_by(source_binding.broker_backend)
        purpose = SimulationLifecycleScheduler._roll_forward_purpose(source_binding.broker_backend)
        metadata.update(
            {
                "source": created_by,
                "purpose": purpose,
                "broker_backend": source_binding.broker_backend.value,
                "target_trade_date": trade_date.isoformat(),
                "extends_release_id": source_release.release_id,
                "extends_binding_id": source_binding.binding_id,
                "new_release_id": new_release.release_id,
                "manifest_identity_source": "strategy_package_current_manifest",
                "source_release_manifest_sha256": source_release.manifest_sha256,
                "authoritative_manifest_sha256": authoritative_manifest_sha256,
                "manifest_identity_changed": source_release.manifest_sha256 != authoritative_manifest_sha256,
                "roll_forward_policy": {
                    "schema_version": "localsim_roll_forward_policy_v1",
                    "immutable_daily_binding": True,
                    "new_strategy_package_supported": True,
                },
            }
        )
        return metadata

    @staticmethod
    def _strategy_name_for_roll_forward(
        *,
        source: SimulationReleaseBinding,
        trade_date: date,
    ) -> str | None:
        if source.broker_backend == SimulationBrokerBackend.LOCAL_SIM:
            return SimulationLifecycleScheduler._localsim_strategy_name_for_trade_date(source.strategy_name, trade_date)
        return source.strategy_name

    @staticmethod
    def _localsim_strategy_name_for_trade_date(strategy_name: str | None, trade_date: date) -> str | None:
        if not strategy_name:
            return strategy_name
        import re

        target = trade_date.isoformat()
        if re.search(r"\d{4}-\d{2}-\d{2}", strategy_name):
            return re.sub(r"\d{4}-\d{2}-\d{2}", target, strategy_name)
        return strategy_name

    def _load_run_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
        as_of_time: datetime | None,
        require_localsim_realtime_quote: bool = False,
    ) -> SimulationRunContext:
        """Load phase-appropriate runtime state without coupling selection to live quotes."""
        phase_loader = getattr(self.context_provider, "load_context_for_phase", None)
        if callable(phase_loader):
            return phase_loader(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
                as_of_time=as_of_time,
                require_localsim_realtime_quote=require_localsim_realtime_quote,
            )
        return self.context_provider.load_context(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
        )

    def _run_binding(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        submit: bool,
        mode: str,
        created_by: str,
        selection_cache: dict[tuple[Any, ...], StrategyPackageSelectionResult | BaseException] | None = None,
        shared_selection_keys: set[tuple[Any, ...]] | None = None,
        as_of_time: datetime | None = None,
    ) -> SimulationSchedulerBindingResult:
        runtime_release = self.repository.get_strategy_runtime_release(binding.release_id)
        existing = self.repository.get_simulation_daily_run_by_key(
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
            trade_date=trade_date,
        )
        if existing is not None and existing.execution_plan_id:
            if self._should_rebuild_localsim_plan_after_side_effect_free_failure(
                binding=binding,
                run=existing,
                submit=submit,
                trade_date=trade_date,
                as_of_time=as_of_time,
            ):
                return self._rebuild_localsim_plan_after_side_effect_free_failure(
                    binding=binding,
                    run=existing,
                    runtime_release=runtime_release,
                    trade_date=trade_date,
                    data_source=data_source,
                    submit=submit,
                    mode=mode,
                    created_by=created_by,
                    selection_cache=selection_cache,
                    shared_selection_keys=shared_selection_keys,
                    as_of_time=as_of_time,
                )
            if self._should_rebuild_miniqmt_plan_after_side_effect_free_failure(binding=binding, run=existing):
                return self._rebuild_miniqmt_plan_after_side_effect_free_failure(
                    binding=binding,
                    run=existing,
                    runtime_release=runtime_release,
                    trade_date=trade_date,
                    data_source=data_source,
                    submit=submit,
                    mode=mode,
                    created_by=created_by,
                    selection_cache=selection_cache,
                    shared_selection_keys=shared_selection_keys,
                    as_of_time=as_of_time,
                )
            return self._existing_plan_result(
                binding=binding,
                run=existing,
                trade_date=trade_date,
                data_source=data_source,
                submit=submit,
                mode=mode,
                as_of_time=as_of_time,
            )

        context = self._load_run_context(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
            as_of_time=as_of_time,
        )
        selection = self._run_selection_once_per_release(
            binding=binding,
            runtime_release=runtime_release,
            trade_date=trade_date,
            data_source=data_source,
            created_by=created_by,
            selection_cache=selection_cache,
            shared_selection_keys=shared_selection_keys,
        )
        self._validate_fresh_selection_evidence(
            binding=binding,
            runtime_release=runtime_release,
            selection=selection,
            trade_date=trade_date,
        )
        build_result = self._build_plan_from_selection(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
            data_source=data_source,
            selection=selection,
            context=context,
            created_by=created_by,
            require_realtime_quote=self._localsim_realtime_quote_required(
                binding=binding,
                trade_date=trade_date,
                submit=submit,
                as_of_time=as_of_time,
            ) if binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM else None,
            as_of_time=as_of_time,
        )
        build_result = self._clear_pre_run_failure_after_planning(build_result)
        build_result = self._persist_no_rebalance_evidence(
            build_result=build_result,
            current_positions=context.current_positions,
        )
        if not submit:
            self._persist_strategy_performance(binding=binding, run=build_result.run, context=context)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="PLANNED",
                run=build_result.run,
                execution_plan=build_result.execution_plan,
                data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )

        try:
            sync_result = self._sync_before_submit(binding=binding, run=build_result.run, context=context)
            build_result, residual_only = self._prepare_localsim_build_result_for_submit(
                binding=binding,
                build_result=build_result,
                context=context,
            )
        except (DataUnavailableError, RuntimeConfigInvalidError) as exc:
            if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
                raise
            marked = self._mark_localsim_pre_submit_retry_failure(
                binding=binding,
                run=build_result.run,
                plan=build_result.execution_plan,
                trade_date=trade_date,
                exc=exc,
            )
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status=marked.status.value,
                run=marked,
                execution_plan=build_result.execution_plan,
                error=self._localsim_pre_submit_error_payload(marked),
                data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        if residual_only is not None:
            self._persist_strategy_performance(binding=binding, run=build_result.run, context=context)
            latest_run = self.repository.get_simulation_daily_run(build_result.run.run_id)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="LOCALSIM_CAPACITY_RESIDUAL_SKIPPED",
                run=latest_run,
                execution_plan=build_result.execution_plan,
                sync_result=sync_result,
                data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        try:
            execution = self._submit_execution_plan_with_timeout(
                build_result=build_result,
                binding=binding,
                run=build_result.run,
                plan=build_result.execution_plan,
                context=context,
                mode=mode,
                as_of_time=as_of_time,
                submit_callable=lambda: self.orchestrator.submit_execution_plan(
                    build_result=build_result,
                    local_broker=context.local_broker,
                    managed_order_service=context.managed_order_service,
                    mode=mode,
                    price_by_symbol=context.price_by_symbol or context.current_prices,
                    miniqmt_runtime_kind=MiniQMTExecutionRuntimeKind.EVENT_LOOP,
                    as_of_time=as_of_time,
                ),
            )
        except BrokerRejectedError as exc:
            terminalized = self._terminalize_deterministic_localsim_submit_failure(
                binding=binding,
                run=build_result.run,
                plan=build_result.execution_plan,
                trade_date=trade_date,
                data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
                exc=exc,
            )
            if terminalized is not None:
                return terminalized
            raise
        local_persistence = self._persist_local_sim_execution_result(
            binding=binding,
            run=execution.run,
            execution=execution,
            context=context,
        )
        if local_persistence is not None and not bool(local_persistence.payload.get("terminal")):
            latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="LOCALSIM_INTRADAY_RUNNING",
                run=latest_run,
                execution_plan=execution.execution_plan,
                execution_result=replace(execution, run=latest_run),
                sync_result=sync_result,
                data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                    binding=binding, trade_date=trade_date, default_data_source=data_source
                ),
            )
        if self._mini_qmt_batch_failed_without_broker_side_effect(execution.run.run_payload_json):
            self._persist_strategy_performance(binding=binding, run=execution.run, context=context)
            latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status=execution.status,
                run=latest_run,
                execution_plan=execution.execution_plan,
                execution_result=execution,
                sync_result=sync_result,
                data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        if (
            binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
            and self._mini_qmt_event_loop_has_pending_algos(execution.run.run_payload_json)
            and not self._mini_qmt_event_loop_has_submitted_children(execution.run.run_payload_json)
        ):
            self._persist_strategy_performance(binding=binding, run=execution.run, context=context)
            latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="MINIQMT_EVENT_LOOP_PENDING",
                run=latest_run,
                execution_plan=execution.execution_plan,
                execution_result=execution,
                sync_result=sync_result,
                data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        tail_result = self._handle_tail_after_submit(binding=binding, run=execution.run, execution=execution, context=context)
        reconciliation = self._reconcile_after_submit_with_timeout(binding=binding, run=execution.run, context=context)
        self._persist_strategy_performance(
            binding=binding,
            run=execution.run,
            context=context,
            local_persistence=local_persistence,
        )
        latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
        status = self._result_status_after_post_submit(execution.status, tail_result=tail_result, reconciliation=reconciliation)
        status = self._local_sim_terminal_capacity_residual_status(latest_run, fallback=status)
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=status,
            run=latest_run,
            execution_plan=execution.execution_plan,
            execution_result=execution,
            sync_result=sync_result,
            reconciliation_result=reconciliation,
            data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                binding=binding,
                trade_date=trade_date,
                default_data_source=data_source,
            ),
        )

    def _rebuild_miniqmt_plan_after_side_effect_free_failure(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        submit: bool,
        mode: str,
        created_by: str,
        selection_cache: dict[tuple[Any, ...], StrategyPackageSelectionResult | BaseException] | None,
        shared_selection_keys: set[tuple[Any, ...]] | None,
        as_of_time: datetime | None,
    ) -> SimulationSchedulerBindingResult:
        rebuild_receipt = self._mini_qmt_side_effect_free_rebuild_receipt(run)
        context = self._load_run_context(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
            as_of_time=as_of_time,
        )
        selection = self._run_selection_once_per_release(
            binding=binding,
            runtime_release=runtime_release,
            trade_date=trade_date,
            data_source=data_source,
            created_by=created_by,
            selection_cache=selection_cache,
            shared_selection_keys=shared_selection_keys,
        )
        self._validate_fresh_selection_evidence(
            binding=binding,
            runtime_release=runtime_release,
            selection=selection,
            trade_date=trade_date,
        )
        build_result = self._build_plan_from_selection(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
            data_source=data_source,
            selection=selection,
            context=context,
            created_by=created_by,
            as_of_time=as_of_time,
        )
        build_result = self._persist_no_rebalance_evidence(
            build_result=build_result,
            current_positions=context.current_positions,
        )
        build_result = replace(
            build_result,
            run=self.repository.update_simulation_daily_run(
                run.run_id,
                status=SimulationDailyRunStatus.PLANNING_EXECUTION,
                selection_evidence=build_result.selection_evidence,
                execution_plan=build_result.execution_plan,
                payload_patch={
                    "last_stage": "PLANNING_EXECUTION",
                    "rebuilt_after_side_effect_free_failure": True,
                    "rebuilt_from_execution_plan_id": run.execution_plan_id,
                    "rebuilt_execution_plan_id": build_result.execution_plan.plan_id,
                    "miniqmt_side_effect_free_rebuild": {
                        **rebuild_receipt,
                        "rebuilt_execution_plan_id": build_result.execution_plan.plan_id,
                    },
                    "miniqmt_context_diagnostics": context.context_diagnostics,
                    "broker_called": False,
                    "submitted_intents": 0,
                    "failed_intents": 0,
                    "qmt_batch_id": None,
                    "qmt_batch_status": None,
                    "qmt_retry_of_batch_id": None,
                    "qmt_batch_result": None,
                    "submit_failure": None,
                },
            ),
        )
        if not submit:
            self._persist_strategy_performance(binding=binding, run=build_result.run, context=context)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="REBUILT_EXISTING_PLAN",
                run=build_result.run,
                execution_plan=build_result.execution_plan,
                data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )

        sync_result = self._sync_before_submit(binding=binding, run=build_result.run, context=context)
        execution = self._submit_execution_plan_with_timeout(
            build_result=build_result,
            binding=binding,
            run=build_result.run,
            plan=build_result.execution_plan,
            context=context,
            mode=mode,
            as_of_time=as_of_time,
            submit_callable=lambda: self.orchestrator.submit_execution_plan(
                build_result=build_result,
                local_broker=context.local_broker,
                managed_order_service=context.managed_order_service,
                mode=mode,
                price_by_symbol=context.price_by_symbol or context.current_prices,
                miniqmt_runtime_kind=MiniQMTExecutionRuntimeKind.EVENT_LOOP,
                as_of_time=as_of_time,
            ),
        )
        local_persistence = self._persist_local_sim_execution_result(
            binding=binding,
            run=execution.run,
            execution=execution,
            context=context,
        )
        if self._mini_qmt_batch_failed_without_broker_side_effect(execution.run.run_payload_json):
            self._persist_strategy_performance(binding=binding, run=execution.run, context=context)
            latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status=execution.status,
                run=latest_run,
                execution_plan=execution.execution_plan,
                execution_result=execution,
                sync_result=sync_result,
                data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        if (
            binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
            and self._mini_qmt_event_loop_has_pending_algos(execution.run.run_payload_json)
            and not self._mini_qmt_event_loop_has_submitted_children(execution.run.run_payload_json)
        ):
            self._persist_strategy_performance(binding=binding, run=execution.run, context=context)
            latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="MINIQMT_EVENT_LOOP_PENDING",
                run=latest_run,
                execution_plan=execution.execution_plan,
                execution_result=execution,
                sync_result=sync_result,
                data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        tail_result = self._handle_tail_after_submit(binding=binding, run=execution.run, execution=execution, context=context)
        reconciliation = self._reconcile_after_submit_with_timeout(binding=binding, run=execution.run, context=context)
        self._persist_strategy_performance(
            binding=binding,
            run=execution.run,
            context=context,
            local_persistence=local_persistence,
        )
        latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
        status = self._result_status_after_post_submit(execution.status, tail_result=tail_result, reconciliation=reconciliation)
        status = self._local_sim_terminal_capacity_residual_status(latest_run, fallback=status)
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=status,
            run=latest_run,
            execution_plan=execution.execution_plan,
            execution_result=execution,
            sync_result=sync_result,
            reconciliation_result=reconciliation,
            data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                binding=binding,
                trade_date=trade_date,
                default_data_source=data_source,
            ),
        )

    def _rebuild_localsim_plan_after_side_effect_free_failure(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        submit: bool,
        mode: str,
        created_by: str,
        selection_cache: dict[tuple[Any, ...], StrategyPackageSelectionResult | BaseException] | None,
        shared_selection_keys: set[tuple[Any, ...]] | None,
        as_of_time: datetime | None,
    ) -> SimulationSchedulerBindingResult:
        context = self._load_run_context(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
            as_of_time=as_of_time,
        )
        selection = self._run_selection_once_per_release(
            binding=binding,
            runtime_release=runtime_release,
            trade_date=trade_date,
            data_source=data_source,
            created_by=created_by,
            selection_cache=selection_cache,
            shared_selection_keys=shared_selection_keys,
        )
        self._validate_fresh_selection_evidence(
            binding=binding,
            runtime_release=runtime_release,
            selection=selection,
            trade_date=trade_date,
        )
        build_result = self._build_plan_from_selection(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
            data_source=data_source,
            selection=selection,
            context=context,
            created_by=created_by,
            require_realtime_quote=self._localsim_realtime_quote_required(
                binding=binding,
                trade_date=trade_date,
                submit=submit,
                as_of_time=as_of_time,
            ),
            as_of_time=as_of_time,
            preserved_causality_cursor=self._local_sim_plan_causality_cursor(
                self.repository.get_execution_plan(run.execution_plan_id or "")
            ),
        )
        build_result = self._persist_no_rebalance_evidence(
            build_result=build_result,
            current_positions=context.current_positions,
        )
        build_result = replace(
            build_result,
            run=self.repository.update_simulation_daily_run(
                run.run_id,
                status=SimulationDailyRunStatus.PLANNING_EXECUTION,
                selection_evidence=build_result.selection_evidence,
                execution_plan=build_result.execution_plan,
                payload_patch={
                    "last_stage": "PLANNING_EXECUTION",
                    "rebuilt_after_side_effect_free_failure": True,
                    "rebuilt_failure_backend": SimulationBrokerBackend.LOCAL_SIM.value,
                    "rebuilt_from_execution_plan_id": run.execution_plan_id,
                    "rebuilt_execution_plan_id": build_result.execution_plan.plan_id,
                    "localsim_context_diagnostics": context.context_diagnostics,
                    "broker_called": False,
                    "submitted_intents": 0,
                },
                payload_unset=("submit_failure",),
            ),
        )
        if not submit:
            self._persist_strategy_performance(binding=binding, run=build_result.run, context=context)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="REBUILT_EXISTING_PLAN",
                run=build_result.run,
                execution_plan=build_result.execution_plan,
                data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )

        try:
            sync_result = self._sync_before_submit(binding=binding, run=build_result.run, context=context)
            build_result, residual_only = self._prepare_localsim_build_result_for_submit(
                binding=binding,
                build_result=build_result,
                context=context,
            )
        except (DataUnavailableError, RuntimeConfigInvalidError) as exc:
            if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
                raise
            marked = self._mark_localsim_pre_submit_retry_failure(
                binding=binding,
                run=build_result.run,
                plan=build_result.execution_plan,
                trade_date=trade_date,
                exc=exc,
            )
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status=marked.status.value,
                run=marked,
                execution_plan=build_result.execution_plan,
                error=self._localsim_pre_submit_error_payload(marked),
                data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        if residual_only is not None:
            self._persist_strategy_performance(binding=binding, run=build_result.run, context=context)
            latest_run = self.repository.get_simulation_daily_run(build_result.run.run_id)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="LOCALSIM_CAPACITY_RESIDUAL_SKIPPED",
                run=latest_run,
                execution_plan=build_result.execution_plan,
                sync_result=sync_result,
                data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        execution = self._submit_execution_plan_with_timeout(
            build_result=build_result,
            binding=binding,
            run=build_result.run,
            plan=build_result.execution_plan,
            context=context,
            mode=mode,
            as_of_time=as_of_time,
            submit_callable=lambda: self.orchestrator.submit_execution_plan(
                build_result=build_result,
                local_broker=context.local_broker,
                managed_order_service=context.managed_order_service,
                mode=mode,
                price_by_symbol=context.price_by_symbol or context.current_prices,
                miniqmt_runtime_kind=MiniQMTExecutionRuntimeKind.EVENT_LOOP,
                as_of_time=as_of_time,
            ),
        )
        local_persistence = self._persist_local_sim_execution_result(
            binding=binding,
            run=execution.run,
            execution=execution,
            context=context,
        )
        if local_persistence is not None and not bool(local_persistence.payload.get("terminal")):
            latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="LOCALSIM_INTRADAY_RUNNING",
                run=latest_run,
                execution_plan=execution.execution_plan,
                execution_result=replace(execution, run=latest_run),
                sync_result=sync_result,
                data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                    binding=binding, trade_date=trade_date, default_data_source=data_source
                ),
            )
        tail_result = self._handle_tail_after_submit(binding=binding, run=execution.run, execution=execution, context=context)
        reconciliation = self._reconcile_after_submit_with_timeout(binding=binding, run=execution.run, context=context)
        self._persist_strategy_performance(
            binding=binding,
            run=execution.run,
            context=context,
            local_persistence=local_persistence,
        )
        latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
        status = self._result_status_after_post_submit(execution.status, tail_result=tail_result, reconciliation=reconciliation)
        status = self._local_sim_terminal_capacity_residual_status(latest_run, fallback=status)
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=status,
            run=latest_run,
            execution_plan=execution.execution_plan,
            execution_result=execution,
            sync_result=sync_result,
            reconciliation_result=reconciliation,
            data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                binding=binding,
                trade_date=trade_date,
                default_data_source=data_source,
            ),
        )

    def _existing_plan_result(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        trade_date: date,
        data_source: str,
        submit: bool,
        mode: str,
        as_of_time: datetime | None,
    ) -> SimulationSchedulerBindingResult:
        plan = self.repository.get_execution_plan(run.execution_plan_id or "")
        runtime_release = self.repository.get_strategy_runtime_release(binding.release_id)
        existing_evidence = self.repository.get_daily_selection_evidence(plan.selection_evidence_id)
        self._validate_fresh_daily_selection_evidence(
            binding=binding,
            runtime_release=runtime_release,
            evidence=existing_evidence,
            trade_date=trade_date,
            runtime_config=StrategyPackageSelectionService.release_selection_runtime_config(runtime_release),
        )
        run = self._recover_legacy_b0_context_missing_run_if_safe(
            binding=binding,
            run=run,
            plan=plan,
            submit=submit,
        )
        status = "REUSED_EXISTING_PLAN"
        if run.status == SimulationDailyRunStatus.SUCCEEDED and not plan.intents:
            status = "NO_REBALANCE"
        local_failure_error = self._local_sim_broker_called_failure_error(binding=binding, run=run)
        if local_failure_error is not None:
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status=run.status.value,
                run=run,
                execution_plan=plan,
                error=local_failure_error,
                data_source=self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        if self._should_mark_existing_no_rebalance(run=run, plan=plan, submit=submit):
            updated = self.repository.update_simulation_daily_run(
                run.run_id,
                status=SimulationDailyRunStatus.SUCCEEDED,
                payload_patch={
                    "no_rebalance_required": True,
                    "broker_called": False,
                    "last_stage": "SUCCEEDED",
                    "submit_failure": None,
                },
            )
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="NO_REBALANCE",
                run=updated,
                execution_plan=plan,
                data_source=self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        if submit and not plan.intents and self._execution_plan_has_pre_trade_blocks(plan):
            execution = self.orchestrator.submit_persisted_execution_plan(
                run=run,
                binding=binding,
                execution_plan=plan,
                mode=mode,
                as_of_time=as_of_time,
            )
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status=execution.status,
                run=execution.run,
                execution_plan=plan,
                execution_result=execution,
                data_source=self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        if self._should_drive_existing_local_sim(binding=binding, run=run, plan=plan, submit=submit):
            return self._drive_existing_local_sim(
                binding=binding,
                run=run,
                plan=plan,
                runtime_release=runtime_release,
                trade_date=trade_date,
                data_source=data_source,
                as_of_time=as_of_time,
            )
        context: SimulationRunContext | None = None
        tick_driver_result = None
        if self._should_drive_existing_miniqmt_event_loop(binding=binding, run=run, submit=submit):
            context = self._load_run_context(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
                as_of_time=as_of_time,
            )
            tick_driver_result = self._drive_miniqmt_event_loop_ticks_with_timeout(
                binding=binding,
                run=run,
                plan=plan,
                context=context,
                mode=mode,
                as_of_time=as_of_time,
            )
            run = self.repository.get_simulation_daily_run(run.run_id)
        if self._should_reconcile_existing_miniqmt_run(binding=binding, run=run, submit=submit):
            if context is None:
                context = self._load_run_context(
                    runtime_release=runtime_release,
                    binding=binding,
                    trade_date=trade_date,
                    as_of_time=as_of_time,
                )
            sync_result = self._sync_before_submit(binding=binding, run=run, context=context)
            reconciliation = self._reconcile_after_submit_with_timeout(binding=binding, run=run, context=context)
            self._persist_strategy_performance(binding=binding, run=run, context=context)
            latest_run = self.repository.get_simulation_daily_run(run.run_id)
            status = self._result_status_after_post_submit(
                "RECOVERED",
                tail_result=None,
                reconciliation=reconciliation,
            )
            if run.status == SimulationDailyRunStatus.SUCCEEDED and status == "RECONCILED":
                status = "REUSED_EXISTING_PLAN"
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status=status,
                run=latest_run,
                execution_plan=plan,
                sync_result=sync_result,
                reconciliation_result=reconciliation,
                data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        if tick_driver_result is not None:
            latest_run = self.repository.get_simulation_daily_run(run.run_id)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="MINIQMT_EVENT_LOOP_TICK_DRIVEN",
                run=latest_run,
                execution_plan=plan,
                data_source=(context.market_data_source if context is not None else None)
                or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        if self._should_submit_existing_plan(binding=binding, run=run, plan=plan, submit=submit):
            runtime_release = self.repository.get_strategy_runtime_release(binding.release_id)
            try:
                context = self._load_run_context(
                    runtime_release=runtime_release,
                    binding=binding,
                    trade_date=trade_date,
                    as_of_time=as_of_time,
                )
                sync_result = self._sync_before_submit(binding=binding, run=run, context=context)
                run, plan, residual_only = self._prepare_localsim_execution_plan_for_submit(
                    binding=binding,
                    run=run,
                    plan=plan,
                    context=context,
                )
            except (DataUnavailableError, RuntimeConfigInvalidError) as exc:
                if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
                    raise
                marked = self._mark_localsim_pre_submit_retry_failure(
                    binding=binding,
                    run=run,
                    plan=plan,
                    trade_date=trade_date,
                    exc=exc,
                )
                return SimulationSchedulerBindingResult(
                    binding_id=binding.binding_id,
                    strategy_id=binding.strategy_id,
                    broker_backend=binding.broker_backend,
                    status=marked.status.value,
                    run=marked,
                    execution_plan=plan,
                    error=self._localsim_pre_submit_error_payload(marked),
                    data_source=self._effective_market_data_source_for_binding(
                        binding=binding,
                        trade_date=trade_date,
                        default_data_source=data_source,
                    ),
                )
            if residual_only is not None:
                self._persist_strategy_performance(binding=binding, run=run, context=context)
                latest_run = self.repository.get_simulation_daily_run(run.run_id)
                return SimulationSchedulerBindingResult(
                    binding_id=binding.binding_id,
                    strategy_id=binding.strategy_id,
                    broker_backend=binding.broker_backend,
                    status="LOCALSIM_CAPACITY_RESIDUAL_SKIPPED",
                    run=latest_run,
                    execution_plan=plan,
                    sync_result=sync_result,
                    data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                        binding=binding,
                        trade_date=trade_date,
                        default_data_source=data_source,
                        ),
                    )
            try:
                execution = self._submit_execution_plan_with_timeout(
                    build_result=None,
                    binding=binding,
                    run=run,
                    plan=plan,
                    context=context,
                    mode=mode,
                    as_of_time=as_of_time,
                    submit_callable=lambda: self.orchestrator.submit_persisted_execution_plan(
                        run=run,
                        binding=binding,
                        execution_plan=plan,
                        local_broker=context.local_broker,
                        managed_order_service=context.managed_order_service,
                        mode=mode,
                        price_by_symbol=context.price_by_symbol or context.current_prices,
                        miniqmt_runtime_kind=MiniQMTExecutionRuntimeKind.EVENT_LOOP,
                        as_of_time=as_of_time,
                    ),
                )
            except BrokerRejectedError as exc:
                terminalized = self._terminalize_deterministic_localsim_submit_failure(
                    binding=binding,
                    run=run,
                    plan=plan,
                    trade_date=trade_date,
                    data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                        binding=binding,
                        trade_date=trade_date,
                        default_data_source=data_source,
                    ),
                    exc=exc,
                )
                if terminalized is not None:
                    return terminalized
                raise
            local_persistence = self._persist_local_sim_execution_result(
                binding=binding,
                run=execution.run,
                execution=execution,
                context=context,
            )
            if local_persistence is not None and not bool(local_persistence.payload.get("terminal")):
                latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
                return SimulationSchedulerBindingResult(
                    binding_id=binding.binding_id,
                    strategy_id=binding.strategy_id,
                    broker_backend=binding.broker_backend,
                    status="LOCALSIM_INTRADAY_RUNNING",
                    run=latest_run,
                    execution_plan=execution.execution_plan,
                    execution_result=replace(execution, run=latest_run),
                    sync_result=sync_result,
                    data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                        binding=binding, trade_date=trade_date, default_data_source=data_source
                    ),
                )
            if self._mini_qmt_batch_failed_without_broker_side_effect(execution.run.run_payload_json):
                self._persist_strategy_performance(binding=binding, run=execution.run, context=context)
                latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
                return SimulationSchedulerBindingResult(
                    binding_id=binding.binding_id,
                    strategy_id=binding.strategy_id,
                    broker_backend=binding.broker_backend,
                    status=execution.status,
                    run=latest_run,
                    execution_plan=execution.execution_plan,
                    execution_result=execution,
                    sync_result=sync_result,
                    data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                        binding=binding,
                        trade_date=trade_date,
                        default_data_source=data_source,
                    ),
                )
            if (
                binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
                and self._mini_qmt_event_loop_has_pending_algos(execution.run.run_payload_json)
                and not self._mini_qmt_event_loop_has_submitted_children(execution.run.run_payload_json)
            ):
                self._persist_strategy_performance(binding=binding, run=execution.run, context=context)
                latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
                return SimulationSchedulerBindingResult(
                    binding_id=binding.binding_id,
                    strategy_id=binding.strategy_id,
                    broker_backend=binding.broker_backend,
                    status="MINIQMT_EVENT_LOOP_PENDING",
                    run=latest_run,
                    execution_plan=execution.execution_plan,
                    execution_result=execution,
                    sync_result=sync_result,
                    data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                        binding=binding,
                        trade_date=trade_date,
                        default_data_source=data_source,
                    ),
                )
            tail_result = self._handle_tail_after_submit(binding=binding, run=execution.run, execution=execution, context=context)
            reconciliation = self._reconcile_after_submit_with_timeout(binding=binding, run=execution.run, context=context)
            self._persist_strategy_performance(
                binding=binding,
                run=execution.run,
                context=context,
                local_persistence=local_persistence,
            )
            latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
            status = self._result_status_after_post_submit(execution.status, tail_result=tail_result, reconciliation=reconciliation)
            status = self._local_sim_terminal_capacity_residual_status(latest_run, fallback=status)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status=status,
                run=latest_run,
                execution_plan=plan,
                execution_result=execution,
                sync_result=sync_result,
                reconciliation_result=reconciliation,
                data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=status,
            run=run,
            execution_plan=plan,
            data_source=self._effective_market_data_source_for_binding(
                binding=binding,
                trade_date=trade_date,
                default_data_source=data_source,
            ),
        )

    def recover_no_side_effect_reconciling_run_after_operator_cleanup(
        self,
        *,
        run_id: str,
        operator_result: Any,
        source: str = "miniqmt_operator_recovery",
    ) -> SimulationDailyRun:
        run = self.require_no_side_effect_reconciling_run_for_operator_recovery(run_id=run_id)
        metadata = dict(getattr(operator_result, "metadata", {}) or {})
        broker_evidence = dict(metadata.get("broker_evidence") or {})
        cleanup = dict(metadata.get("runtime_only_cleanup") or {})
        command_id = str(getattr(operator_result, "command_id", "") or "")
        command_type = str(getattr(operator_result, "command_type", "") or "")
        status = str(getattr(getattr(operator_result, "status", None), "value", getattr(operator_result, "status", "")))
        open_order_count = int(broker_evidence.get("broker_open_order_count") or 0)
        if status != "EXECUTED" or open_order_count != 0 or metadata.get("broker_mutated") is True:
            raise RuntimeConfigInvalidError(
                "MiniQMT stale runtime recovery requires executed broker-empty operator evidence; "
                "reason_code=MINIQMT_STALE_RUNTIME_RECOVERY_OPERATOR_EVIDENCE_REJECTED",
                context={
                    "reason_code": "MINIQMT_STALE_RUNTIME_RECOVERY_OPERATOR_EVIDENCE_REJECTED",
                    "run_id": run.run_id,
                    "operator_status": status,
                    "broker_open_order_count": open_order_count,
                    "broker_mutated": metadata.get("broker_mutated"),
                    "command_id": command_id,
                },
            )
        plan = self.repository.get_execution_plan(run.execution_plan_id or "")
        plan_counts = self._execution_plan_side_counts(plan)
        diagnostic = {
            "schema_version": "miniqmt_no_side_effect_reconciling_recovery_v1",
            "reason_code": "MINIQMT_NO_SIDE_EFFECT_RECONCILING_RECOVERED",
            "run_id": run.run_id,
            "plan_id": plan.plan_id,
            "strategy_id": run.strategy_id,
            "binding_id": run.binding_id,
            "trade_date": run.trade_date.isoformat(),
            "previous_status": run.status.value,
            "next_status": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
            "source": source,
            "operator_command_id": command_id,
            "operator_command_type": command_type,
            "broker_evidence": broker_evidence,
            "runtime_only_cleanup": cleanup,
            "broker_called": False,
            "submitted_intents": 0,
            "order_intent_count": run.run_payload_json.get("order_intent_count", plan_counts["intent_count"]),
            "message": (
                "RECONCILING run had no broker side effect and broker authority was empty; runtime-only stale "
                "state was terminalized so the next scheduler tick can reuse the standard FAILED_RETRYABLE path"
            ),
        }
        return self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.FAILED_RETRYABLE,
            payload_patch={
                "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
                "broker_called": False,
                "submitted_intents": 0,
                "failed_intents": 0,
                "no_rebalance_required": False,
                "miniqmt_no_side_effect_reconciling_recovery": diagnostic,
                "submit_failure": {
                    "stage": "MINIQMT_NO_SIDE_EFFECT_RECONCILING_RECOVERY",
                    "type": "OperatorRecovery",
                    "message": diagnostic["message"],
                    "context": diagnostic,
                },
            },
        )

    def require_no_side_effect_reconciling_run_for_operator_recovery(self, *, run_id: str) -> SimulationDailyRun:
        run = self.repository.get_simulation_daily_run(run_id)
        if run.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
            raise RuntimeConfigInvalidError(
                "MiniQMT stale runtime recovery requires a MiniQMT SIM run; "
                "reason_code=MINIQMT_STALE_RUNTIME_RECOVERY_RUN_BACKEND_UNSUPPORTED",
                context={
                    "reason_code": "MINIQMT_STALE_RUNTIME_RECOVERY_RUN_BACKEND_UNSUPPORTED",
                    "run_id": run.run_id,
                    "broker_backend": run.broker_backend.value,
                },
            )
        if run.status != SimulationDailyRunStatus.RECONCILING:
            raise RuntimeConfigInvalidError(
                "MiniQMT stale runtime recovery only accepts RECONCILING runs; "
                "reason_code=MINIQMT_STALE_RUNTIME_RECOVERY_RUN_STATUS_UNSUPPORTED",
                context={
                    "reason_code": "MINIQMT_STALE_RUNTIME_RECOVERY_RUN_STATUS_UNSUPPORTED",
                    "run_id": run.run_id,
                    "status": run.status.value,
                },
            )
        if run.run_payload_json.get("broker_called") is not False or int(run.run_payload_json.get("submitted_intents") or 0) != 0:
            raise RuntimeConfigInvalidError(
                "MiniQMT stale runtime recovery requires a no-side-effect run; "
                "reason_code=MINIQMT_STALE_RUNTIME_RECOVERY_RUN_HAS_SIDE_EFFECT_EVIDENCE",
                context={
                    "reason_code": "MINIQMT_STALE_RUNTIME_RECOVERY_RUN_HAS_SIDE_EFFECT_EVIDENCE",
                    "run_id": run.run_id,
                    "broker_called": run.run_payload_json.get("broker_called"),
                    "submitted_intents": run.run_payload_json.get("submitted_intents"),
                },
            )
        return run

    def _mark_localsim_pre_submit_retry_failure(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        trade_date: date,
        exc: BaseException,
    ) -> SimulationDailyRun:
        plan_counts = self._execution_plan_side_counts(plan)
        context = getattr(exc, "context", None)
        if not isinstance(context, dict):
            context = {}
        stage = self._localsim_pre_submit_failure_stage(exc)
        diagnostic = {
            "schema_version": "localsim_pre_submit_retry_diagnostics_v1",
            "stage": stage,
            "reason_code": str(context.get("reason_code") or stage),
            "reason": "local_sim_retry_failed_before_broker_submit",
            "run_id": run.run_id,
            "plan_id": plan.plan_id,
            "strategy_id": binding.strategy_id,
            "binding_id": binding.binding_id,
            "trade_date": trade_date.isoformat(),
            "plan_intent_count": plan_counts["intent_count"],
            "buy_intent_count": plan_counts["buy_intent_count"],
            "sell_intent_count": plan_counts["sell_intent_count"],
            "error_type": type(exc).__name__,
            "message": str(exc),
            "context": context,
            "next_action": (
                "fix the LocalSim context, cash, price, or market-data dependency and rerun the next scheduler "
                "execution tick; no broker order was submitted in this failed retry"
            ),
        }
        return self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.FAILED_RETRYABLE,
            payload_patch={
                "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
                "no_rebalance_required": False,
                "broker_called": False,
                "submitted_intents": 0,
                "failed_intents": plan_counts["intent_count"],
                "local_sim_retry_diagnostics": diagnostic,
                "submit_failure": {
                    "stage": diagnostic["stage"],
                    "type": diagnostic["error_type"],
                    "message": diagnostic["message"],
                    "context": diagnostic,
                },
            },
        )

    def _terminalize_deterministic_localsim_submit_failure(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        trade_date: date,
        data_source: str,
        exc: BrokerRejectedError,
    ) -> SimulationSchedulerBindingResult | None:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
            return None
        if not self._is_deterministic_localsim_submit_failure(exc):
            return None
        plan_counts = self._execution_plan_side_counts(plan)
        raw_context = getattr(exc, "context", None)
        context = dict(raw_context) if isinstance(raw_context, dict) else {}
        reason_code = self._deterministic_localsim_submit_reason_code(context)
        diagnostic = {
            "schema_version": "localsim_deterministic_submit_failure_v1",
            "stage": "LOCAL_SIM_SUBMIT_FAILED",
            "reason_code": reason_code,
            "reason": "local_sim_deterministic_submit_failure_terminalized",
            "run_id": run.run_id,
            "plan_id": plan.plan_id,
            "strategy_id": binding.strategy_id,
            "binding_id": binding.binding_id,
            "trade_date": trade_date.isoformat(),
            "plan_intent_count": plan_counts["intent_count"],
            "buy_intent_count": plan_counts["buy_intent_count"],
            "sell_intent_count": plan_counts["sell_intent_count"],
            "error_type": type(exc).__name__,
            "message": str(exc),
            "context": context,
            "next_action": (
                "fix the deterministic LocalSim order-shape rejection before retrying; "
                "the scheduler will not repeatedly resubmit the same failed plan"
            ),
        }
        updated = self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.FAILED_TERMINAL,
            payload_patch={
                "last_stage": SimulationDailyRunStatus.FAILED_TERMINAL.value,
                "no_rebalance_required": False,
                "broker_called": False,
                "submitted_intents": 0,
                "failed_intents": plan_counts["intent_count"],
                "local_sim_retry_diagnostics": diagnostic,
                "local_sim_deterministic_submit_failure": diagnostic,
                "submit_failure": {
                    "stage": "LOCAL_SIM_SUBMIT_FAILED",
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "context": diagnostic,
                },
            },
        )
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=updated.status.value,
            run=updated,
            execution_plan=plan,
            error={
                "type": type(exc).__name__,
                "message": str(exc),
                "context": diagnostic,
            },
            data_source=data_source,
        )

    @staticmethod
    def _execution_plan_side_counts(plan: ExecutionPlan) -> dict[str, int]:
        return {
            "intent_count": len(plan.intents),
            "buy_intent_count": sum(1 for intent in plan.intents if intent.side == OrderSide.BUY),
            "sell_intent_count": sum(1 for intent in plan.intents if intent.side == OrderSide.SELL),
        }

    @staticmethod
    def _localsim_pre_submit_failure_stage(exc: BaseException) -> str:
        context = getattr(exc, "context", None)
        if isinstance(context, dict) and context.get("reason_code") == "LOCALSIM_CASH_CONTEXT_MISSING":
            return "LOCAL_SIM_CASH_CONTEXT_MISSING"
        message = str(exc).lower()
        if "cash" in message:
            return "LOCAL_SIM_CASH_CONTEXT_MISSING"
        if "market data" in message or "minute" in message:
            return "LOCAL_SIM_MARKET_DATA_UNAVAILABLE"
        if "price" in message:
            return "LOCAL_SIM_PRICE_UNAVAILABLE"
        return "LOCAL_SIM_CONTEXT_UNAVAILABLE"

    @staticmethod
    def _localsim_pre_submit_error_payload(run: SimulationDailyRun) -> dict[str, Any]:
        failure = run.run_payload_json.get("submit_failure")
        if not isinstance(failure, dict):
            return {
                "type": run.status.value,
                "message": run.status.value,
                "context": {"run_id": run.run_id},
            }
        return {
            "type": str(failure.get("type") or run.status.value),
            "message": str(failure.get("message") or run.status.value),
            "context": failure.get("context") if isinstance(failure.get("context"), dict) else {"run_id": run.run_id},
        }

    @staticmethod
    def _local_sim_broker_called_failure_error(
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
    ) -> dict[str, Any] | None:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
            return None
        if run.status not in {SimulationDailyRunStatus.FAILED_RETRYABLE, SimulationDailyRunStatus.FAILED_TERMINAL}:
            return None
        if not bool(run.run_payload_json.get("broker_called")):
            return None
        failure = run.run_payload_json.get("submit_failure")
        return {
            "type": str(failure.get("type") or run.status.value) if isinstance(failure, dict) else run.status.value,
            "message": str(failure.get("message") or "LocalSim broker-called run is failed") if isinstance(failure, dict) else "LocalSim broker-called run is failed",
            "context": failure.get("context") if isinstance(failure, dict) else {"run_id": run.run_id},
        }

    def _prepare_localsim_build_result_for_submit(
        self,
        *,
        binding: SimulationReleaseBinding,
        build_result: SimulationPlanBuildResult,
        context: SimulationRunContext,
    ) -> tuple[SimulationPlanBuildResult, dict[str, Any] | None]:
        run, plan, residual_only = self._prepare_localsim_execution_plan_for_submit(
            binding=binding,
            run=build_result.run,
            plan=build_result.execution_plan,
            context=context,
        )
        if run is build_result.run and plan is build_result.execution_plan:
            return build_result, residual_only
        return replace(build_result, run=run, execution_plan=plan), residual_only

    def _prepare_localsim_execution_plan_for_submit(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        context: SimulationRunContext,
    ) -> tuple[SimulationDailyRun, ExecutionPlan, dict[str, Any] | None]:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM or not plan.intents:
            return run, plan, None
        prepared_plan, fit_payload = self._cash_fit_localsim_execution_plan(
            binding=binding,
            run=run,
            plan=plan,
            context=context,
        )
        if fit_payload["status"] == "UNCHANGED":
            return run, plan, None
        if fit_payload["prepared_intent_count"] > 0:
            prepared_plan = self.repository.save_execution_plan(prepared_plan)
            updated = self.repository.update_simulation_daily_run(
                run.run_id,
                execution_plan=prepared_plan,
                payload_patch={
                    "local_sim_cash_fit": fit_payload,
                    "execution_plan_intent_count": len(prepared_plan.intents),
                    "order_intent_count": len(prepared_plan.intents),
                },
                payload_unset=("submit_failure", "local_sim_retry_diagnostics"),
            )
            return updated, prepared_plan, None

        updated = self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.FAILED_TERMINAL,
            payload_patch={
                "local_sim_cash_fit": fit_payload,
                "no_rebalance_required": False,
                "broker_called": False,
                "submitted_intents": 0,
                "last_stage": "FAILED_TERMINAL",
            },
            payload_unset=("submit_failure", "local_sim_retry_diagnostics"),
        )
        return updated, plan, fit_payload

    def _cash_fit_localsim_execution_plan(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        context: SimulationRunContext,
    ) -> tuple[ExecutionPlan, dict[str, Any]]:
        if context.cash is None:
            raise DataUnavailableError(
                "LocalSim cash-fit requires explicit account cash; context.cash is missing",
                context={
                    "reason_code": "LOCALSIM_CASH_CONTEXT_MISSING",
                    "stage": "LOCALSIM_CASH_FIT",
                    "run_id": run.run_id,
                    "plan_id": plan.plan_id,
                    "binding_id": binding.binding_id,
                    "strategy_id": binding.strategy_id,
                    "trade_date": run.trade_date.isoformat(),
                    "broker_backend": binding.broker_backend.value,
                    "required_action": "load authoritative Paper v2 portfolio cash before LocalSim submit; do not default missing cash to 0.0",
                },
            )
        cash = float(context.cash)
        running_cash = max(0.0, cash)
        sells = [intent for intent in plan.intents if intent.side == OrderSide.SELL]
        buys = [intent for intent in plan.intents if intent.side == OrderSide.BUY]
        prepared = [*sells]
        simulated_sell_proceeds = 0.0
        for intent in sells:
            price = self._reference_price_for_localsim_cash_fit(intent, context=context)
            proceeds = max(0.0, int(intent.order_quantity) * price - self._estimated_localsim_fee(intent, price))
            proceeds *= _LOCALSIM_CASH_FIT_SELL_PROCEEDS_BUFFER_RATIO
            simulated_sell_proceeds += proceeds
            running_cash += proceeds

        skipped: list[dict[str, Any]] = []
        submitted_buy_count = 0
        for intent in buys:
            price = self._reference_price_for_localsim_cash_fit(intent, context=context)
            required_cash = (
                int(intent.order_quantity) * price + self._estimated_localsim_fee(intent, price)
            ) * _LOCALSIM_CASH_FIT_BUY_BUFFER_RATIO
            if required_cash <= running_cash + 1e-8:
                prepared.append(intent)
                submitted_buy_count += 1
                running_cash -= required_cash
                continue
            skipped.append(
                {
                    "intent_id": intent.intent_id,
                    "symbol": intent.symbol,
                    "side": intent.side.value,
                    "order_quantity": int(intent.order_quantity),
                    "required_cash": round(required_cash, 6),
                    "cash_before_intent": round(running_cash, 6),
                    "reason_code": "SKIPPED_INSUFFICIENT_CAPITAL",
                    "next_action": "rebuild the daily LocalSim plan on the next scheduler tick after sell proceeds or cash state changes",
                }
            )

        payload = {
            "schema_version": "localsim_cash_fit_v1",
            "status": "UNCHANGED",
            "reason": "localsim_cash_fit_sell_first_proceeds_aware",
            "initial_cash": round(cash, 6),
            "simulated_sell_proceeds": round(simulated_sell_proceeds, 6),
            "remaining_cash_buffer": round(running_cash, 6),
            "original_intent_count": len(plan.intents),
            "prepared_intent_count": len(prepared),
            "sell_intent_count": len(sells),
            "buy_intent_count": len(buys),
            "submitted_buy_count": submitted_buy_count,
            "skipped_buy_count": len(skipped),
            "skipped_buy_intents": skipped,
        }
        original_ids = [intent.intent_id for intent in plan.intents]
        prepared_ids = [intent.intent_id for intent in prepared]
        if not skipped and prepared_ids == original_ids:
            return plan, payload
        if skipped:
            payload["status"] = "CAPACITY_RESIDUAL_SKIPPED"
        else:
            payload["status"] = "SELL_FIRST_REORDERED"
        prepared_plan = self._copy_localsim_plan_with_intents(
            plan=plan,
            intents=prepared,
            cash_fit_payload=payload,
        )
        return prepared_plan, payload

    @staticmethod
    def _reference_price_for_localsim_cash_fit(intent: Any, *, context: SimulationRunContext) -> float:
        for value in (
            intent.price_policy.get("reference_price"),
            intent.price_policy.get("limit_price"),
            (context.price_by_symbol or {}).get(intent.symbol),
            context.current_prices.get(intent.symbol),
        ):
            if value is None:
                continue
            price = float(value)
            if price > 0:
                return price
        raise DataUnavailableError(
            "LocalSim cash-fit requires a positive reference price",
            context={
                "intent_id": intent.intent_id,
                "symbol": intent.symbol,
                "plan_id": intent.plan_id,
            },
        )

    @staticmethod
    def _estimated_localsim_fee(intent: Any, price: float) -> float:
        rate = _LOCALSIM_DEFAULT_OPEN_COST if intent.side == OrderSide.BUY else _LOCALSIM_DEFAULT_CLOSE_COST
        return max(int(intent.order_quantity) * price * rate, _LOCALSIM_DEFAULT_MIN_FEE)

    @staticmethod
    def _copy_localsim_plan_with_intents(
        *,
        plan: ExecutionPlan,
        intents: list[Any],
        cash_fit_payload: dict[str, Any],
    ) -> ExecutionPlan:
        payload = deepcopy(plan.plan_payload_json)
        payload["intents"] = [
            {
                "intent_id": intent.intent_id,
                "symbol": intent.symbol,
                "side": intent.side.value,
                "target_quantity": intent.target_quantity,
                "delta_quantity": intent.delta_quantity,
                "order_quantity": intent.order_quantity,
                "target_weight": intent.target_weight,
                "reference_price": intent.price_policy.get("reference_price"),
                "current_quantity": intent.current_quantity,
                "current_available_quantity": intent.current_available_quantity,
                "rebalance_reason": intent.rebalance_reason,
                "trading_rule_decision_id": intent.trading_rule_decision_id,
                "order_type": intent.price_policy.get("order_type"),
                "limit_price": intent.price_policy.get("limit_price"),
                "schedule_window": intent.schedule_window,
                "price_policy": intent.price_policy,
                "risk_context": intent.risk_context,
                "metadata": intent.metadata,
            }
            for intent in intents
        ]
        payload["local_sim_cash_fit"] = cash_fit_payload
        plan_hash = canonical_json_sha256(payload)
        plan_id = f"plan_{plan_hash[:16]}"
        plan_intents = [
            intent.model_copy(update={"plan_id": plan_id})
            for intent in intents
        ]
        return plan.model_copy(
            update={
                "plan_id": plan_id,
                "intents": plan_intents,
                "plan_payload_json": payload,
                "plan_hash": plan_hash,
                "created_at": datetime.now(UTC),
            }
        )

    @staticmethod
    def _effective_market_data_source_for_binding(
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        default_data_source: str,
    ) -> str:
        if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM:
            return MinuteDataSource.MINIQMT_REALTIME.value
        if binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM and trade_date == date.today():
            return MinuteDataSource.TDX_REALTIME.value
        return default_data_source

    @staticmethod
    def _should_rebuild_miniqmt_plan_after_side_effect_free_failure(
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
    ) -> bool:
        return (
            binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
            and run.status in {SimulationDailyRunStatus.FAILED_RETRYABLE, SimulationDailyRunStatus.SUCCEEDED}
            and SimulationLifecycleScheduler._mini_qmt_batch_failed_without_broker_side_effect(run.run_payload_json)
        )

    def _should_rebuild_localsim_plan_after_side_effect_free_failure(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        submit: bool,
        trade_date: date,
        as_of_time: datetime | None,
    ) -> bool:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
            return False
        if not submit:
            return False
        if bool(run.run_payload_json.get("broker_called")):
            return False
        if not self._localsim_realtime_quote_required(
            binding=binding,
            trade_date=trade_date,
            submit=submit,
            as_of_time=as_of_time,
        ):
            return False
        if run.status == SimulationDailyRunStatus.FAILED_RETRYABLE:
            failure = run.run_payload_json.get("submit_failure")
            if isinstance(failure, dict) and failure.get("stage") == "LOCAL_SIM_SUBMIT_FAILED":
                context = failure.get("context") if isinstance(failure.get("context"), dict) else {}
                text = " ".join(
                    str(item or "")
                    for item in (
                        failure.get("message"),
                        context.get("cause"),
                        context.get("cause_code"),
                    )
                ).lower()
                if "insufficient cash" in text:
                    return True
        return (
            callable(getattr(self.context_provider, "load_context_for_phase", None))
            and run.status in {
                SimulationDailyRunStatus.PLANNING_EXECUTION,
                SimulationDailyRunStatus.FAILED_RETRYABLE,
            }
        )

    @staticmethod
    def _localsim_realtime_quote_required(
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        submit: bool,
        as_of_time: datetime | None,
    ) -> bool:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM or not submit:
            return False
        local_as_of = scheduler_time(as_of_time)
        if trade_date != local_as_of.date():
            return False
        return any(
            window.get("state") == "ACTIVE" and window.get("action") == "submit"
            for window in compute_schedule_windows(trade_date=trade_date, as_of_time=local_as_of)
        )

    @staticmethod
    def _is_deterministic_localsim_submit_failure(exc: BaseException) -> bool:
        if not isinstance(exc, BrokerRejectedError):
            return False
        context = getattr(exc, "context", None)
        if not isinstance(context, dict):
            return False
        cause = str(context.get("cause") or "").lower()
        cause_code = str(context.get("cause_code") or "").upper()
        if "LOCAL_SIM_BOARD_LOT_VIOLATION" in cause_code or "LOCAL_SIM_BOARD_LOT_VIOLATION".lower() in cause:
            return True
        return False

    @staticmethod
    def _deterministic_localsim_submit_reason_code(context: dict[str, Any]) -> str:
        cause_code = str(context.get("cause_code") or "").upper()
        cause = str(context.get("cause") or "").upper()
        if "LOCAL_SIM_BOARD_LOT_VIOLATION" in cause_code or "LOCAL_SIM_BOARD_LOT_VIOLATION" in cause:
            return "LOCAL_SIM_BOARD_LOT_VIOLATION"
        return "LOCAL_SIM_DETERMINISTIC_SUBMIT_REJECTED"

    @staticmethod
    def _mini_qmt_batch_failed_without_broker_side_effect(payload: dict[str, Any]) -> bool:
        if bool(payload.get("broker_called")):
            return False
        if SimulationLifecycleScheduler._mini_qmt_batch_has_broker_side_effect_evidence(payload):
            return False
        if SimulationLifecycleScheduler._mini_qmt_batch_has_duplicate_order_remark(payload):
            return False
        batch = payload.get("qmt_batch_result") if isinstance(payload.get("qmt_batch_result"), dict) else {}
        status = str(payload.get("qmt_batch_status") or batch.get("batch_status") or "").upper()
        if status in {"PREFLIGHT_FAILED", "FAILED", "PARTIAL"}:
            try:
                failed = int(batch.get("failed", payload.get("failed_intents", 0)) or 0)
            except (TypeError, ValueError):
                failed = 0
            if failed > 0:
                return True
        return SimulationLifecycleScheduler._b0_manifest_conflict_requires_plan_rebuild(payload)

    @staticmethod
    def _b0_manifest_conflict_requires_plan_rebuild(payload: dict[str, Any]) -> bool:
        failure = payload.get("submit_failure")
        if not isinstance(failure, dict):
            return False
        context = failure.get("context")
        if not isinstance(context, dict):
            return False
        if str(context.get("reason_code") or "").upper() != "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT":
            return False
        conflicts = context.get("manifest_conflicts")
        if not isinstance(conflicts, dict) or not conflicts:
            return False
        fingerprint = canonical_json_sha256(
            {
                "schema_version": "miniqmt_b0_manifest_conflict_rebuild_v1",
                "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
                "manifest_conflicts": conflicts,
            }
        )
        previous = payload.get("miniqmt_side_effect_free_rebuild")
        if isinstance(previous, dict) and previous.get("failure_fingerprint") == fingerprint:
            # The same immutable conflict after one fresh-plan rebuild means
            # the runtime and compiler roots genuinely disagree. Rebuilding
            # forever would hide that deployment fault, so retain the loud
            # failure until the observed conflict changes.
            return False
        return True

    @staticmethod
    def _mini_qmt_side_effect_free_rebuild_receipt(run: SimulationDailyRun) -> dict[str, Any]:
        payload = run.run_payload_json if isinstance(run.run_payload_json, dict) else {}
        failure = payload.get("submit_failure") if isinstance(payload.get("submit_failure"), dict) else {}
        context = failure.get("context") if isinstance(failure.get("context"), dict) else {}
        reason_code = str(context.get("reason_code") or "MINIQMT_BATCH_SIDE_EFFECT_FREE_FAILURE").upper()
        receipt: dict[str, Any] = {
            "schema_version": "miniqmt_side_effect_free_plan_rebuild_v1",
            "reason_code": reason_code,
            "source_execution_plan_id": run.execution_plan_id,
            "broker_called": False,
        }
        conflicts = context.get("manifest_conflicts")
        if reason_code == "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT" and isinstance(conflicts, dict) and conflicts:
            receipt["manifest_conflicts"] = deepcopy(conflicts)
            receipt["failure_fingerprint"] = canonical_json_sha256(
                {
                    "schema_version": "miniqmt_b0_manifest_conflict_rebuild_v1",
                    "reason_code": reason_code,
                    "manifest_conflicts": conflicts,
                }
            )
        return receipt

    def _run_selection_once_per_release(
        self,
        *,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        created_by: str,
        selection_cache: dict[tuple[Any, ...], StrategyPackageSelectionResult | BaseException] | None,
        shared_selection_keys: set[tuple[Any, ...]] | None,
    ) -> StrategyPackageSelectionResult:
        runtime_config = StrategyPackageSelectionService.release_selection_runtime_config(runtime_release)
        cache_key = self._selection_cache_key(
            binding=binding,
            trade_date=trade_date,
            data_source=data_source,
        )
        if shared_selection_keys is not None and cache_key not in shared_selection_keys:
            selection_cache = None
        if selection_cache is not None and cache_key in selection_cache:
            cached = selection_cache[cache_key]
            if isinstance(cached, BaseException):
                raise cached
            return cached
        try:
            if self._selection_artifact_auto_generation_enabled(runtime_config):
                selection = self._run_auto_generated_selection_nonblocking(
                    binding=binding,
                    runtime_release=runtime_release,
                    trade_date=trade_date,
                    data_source=data_source,
                    runtime_config=runtime_config,
                    created_by=created_by,
                )
            else:
                selection = self._run_selection_sync(
                    binding=binding,
                    runtime_release=runtime_release,
                    trade_date=trade_date,
                    data_source=data_source,
                    runtime_config=runtime_config,
                    created_by=created_by,
                )
        except Exception as exc:
            if selection_cache is not None:
                selection_cache[cache_key] = exc
            raise
        if selection_cache is not None:
            selection_cache[cache_key] = selection
        return selection

    def _run_selection_sync(
        self,
        *,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
        created_by: str,
    ) -> StrategyPackageSelectionResult:
        return self.selection_service.run_selection(
            package_ids=[binding.package_id],
            mode=SelectionMode.SINGLE_PACKAGE,
            trade_date=trade_date,
            data_source=data_source,
            runtime_config=runtime_config,
            runtime_release=runtime_release,
            created_by=created_by,
        )

    def _run_auto_generated_selection_nonblocking(
        self,
        *,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
        created_by: str,
    ) -> StrategyPackageSelectionResult:
        if self._authoritative_selection_artifact_exists(
            binding=binding,
            runtime_release=runtime_release,
            trade_date=trade_date,
            data_source=data_source,
            runtime_config=runtime_config,
        ):
            return self._run_selection_sync(
                binding=binding,
                runtime_release=runtime_release,
                trade_date=trade_date,
                data_source=data_source,
                runtime_config=runtime_config,
                created_by=created_by,
            )

        key = self._selection_inference_key(
            binding=binding,
            trade_date=trade_date,
            data_source=data_source,
            runtime_config=runtime_config,
        )
        now = monotonic_time.monotonic()
        with self._selection_inference_lock:
            if self._selection_inference_shutdown:
                raise RuntimeConfigInvalidError(
                    "simulation scheduler selection inference executor is shut down",
                    context={
                        **self._selection_inference_context(
                            binding=binding,
                            runtime_release=runtime_release,
                            trade_date=trade_date,
                            data_source=data_source,
                            runtime_config=runtime_config,
                        ),
                        "reason_code": "SIMULATION_SELECTION_INFERENCE_EXECUTOR_SHUTDOWN",
                        "failure_stage": "SELECTION_INFERENCE",
                    },
                )
            entry = self._selection_inference_inflight.get(key)
            if entry is None:
                context = self._selection_inference_context(
                    binding=binding,
                    runtime_release=runtime_release,
                    trade_date=trade_date,
                    data_source=data_source,
                    runtime_config=runtime_config,
                )
                future = self._selection_inference_executor.submit(
                    self._run_selection_sync,
                    binding=binding,
                    runtime_release=runtime_release,
                    trade_date=trade_date,
                    data_source=data_source,
                    runtime_config=deepcopy(runtime_config),
                    created_by=created_by,
                )
                entry = _SelectionInferenceInFlight(
                    key=key,
                    future=future,
                    started_monotonic=now,
                    started_at=datetime.now(UTC).isoformat(),
                    context=context,
                )
                self._selection_inference_inflight[key] = entry
                logger.warning(
                    "Simulation scheduler dispatched auto-generated selection inference off tick thread: %s",
                    context,
                )
            if entry.future.done():
                self._selection_inference_inflight.pop(key, None)
                future = entry.future
            else:
                elapsed = now - entry.started_monotonic
                if elapsed >= self._selection_inference_timeout_seconds:
                    entry.timed_out = True
                    raise self._selection_inference_timeout_error(entry, elapsed_seconds=elapsed)
                raise self._selection_inference_pending_error(entry, elapsed_seconds=elapsed)
        return future.result()

    @staticmethod
    def _selection_artifact_auto_generation_enabled(runtime_config: dict[str, Any]) -> bool:
        artifact_config = StrategyPackageSelectionService.selection_artifact_config(runtime_config)
        return bool(artifact_config.get("auto_generate"))

    def _authoritative_selection_artifact_exists(
        self,
        *,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
    ) -> bool:
        artifact_config = StrategyPackageSelectionService.selection_artifact_config(runtime_config)
        if bool(artifact_config.get("force_regenerate")):
            return False
        artifact_repository = self._selection_artifact_repository()
        if artifact_repository is None:
            return False
        runtime_hashes = self._selection_artifact_runtime_hashes(
            binding=binding,
            runtime_release=runtime_release,
            runtime_config=runtime_config,
        )
        for runtime_hash in runtime_hashes:
            try:
                artifact = artifact_repository.get(
                    package_id=binding.package_id,
                    manifest_sha256=binding.manifest_sha256 or runtime_release.manifest_sha256 or "",
                    trade_date=trade_date,
                    data_source=data_source,
                    runtime_config_hash=runtime_hash,
                )
            except DataUnavailableError:
                continue
            metadata = artifact.metadata or {}
            if (
                artifact.status.value == "SUCCEEDED"
                and artifact.scores_json
                and metadata.get("source_type") == AUTHORITATIVE_SELECTION_SOURCE_TYPE
                and metadata.get("authority_scope") == AUTHORITATIVE_SELECTION_SCOPE
            ):
                return True
        return False

    def _selection_artifact_runtime_hashes(
        self,
        *,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        runtime_config: dict[str, Any],
    ) -> list[str]:
        hashes = list(_candidate_selection_artifact_runtime_hashes(runtime_config))
        manifest = self._selection_package_manifest(binding=binding, runtime_release=runtime_release)
        if manifest is not None and manifest.alpha_mode == AlphaMode.MULTI_ALPHA:
            multi_hash = multi_alpha_selection_artifact_runtime_hash(manifest, runtime_config)
            hashes = [multi_hash, *[item for item in hashes if item != multi_hash]]
        return hashes

    def _selection_package_manifest(
        self,
        *,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
    ) -> StrategyPackageManifest | None:
        package_repository = getattr(self.selection_service, "package_repository", None)
        getter = getattr(package_repository, "get", None)
        if not callable(getter):
            return None
        record = getter(binding.package_id)
        manifest = record.current_manifest()
        if not manifest.manifest_sha256:
            return manifest.model_copy(update={"manifest_sha256": binding.manifest_sha256 or runtime_release.manifest_sha256})
        return manifest

    def _selection_artifact_repository(self) -> Any | None:
        candidates = [
            getattr(self.selection_service, "runtime", None),
            getattr(getattr(self.selection_service, "signal_service", None), "runtime", None),
            getattr(self.selection_service, "selection_artifact_service", None),
            getattr(getattr(self.selection_service, "signal_service", None), "selection_artifact_service", None),
        ]
        for candidate in candidates:
            artifact_repository = getattr(candidate, "artifact_repository", None)
            if artifact_repository is not None and callable(getattr(artifact_repository, "get", None)):
                return artifact_repository
        return None

    def _selection_inference_key(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
    ) -> tuple[Any, ...]:
        return (
            binding.package_id,
            binding.manifest_sha256,
            trade_date.isoformat(),
            data_source,
            selection_artifact_runtime_hash(runtime_config),
        )

    def _selection_inference_context(
        self,
        *,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "stage": "SELECTION_INFERENCE",
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "package_id": binding.package_id,
            "manifest_sha256": binding.manifest_sha256 or runtime_release.manifest_sha256,
            "release_id": runtime_release.release_id,
            "release_hash": runtime_release.release_hash,
            "trade_date": trade_date.isoformat(),
            "data_source": data_source,
            "runtime_config_hash": selection_artifact_runtime_hash(runtime_config),
        }

    def _selection_inference_pending_error(
        self,
        entry: _SelectionInferenceInFlight,
        *,
        elapsed_seconds: float,
    ) -> DataUnavailableError:
        return DataUnavailableError(
            "simulation scheduler selection inference is running asynchronously; tick will continue",
            context={
                **entry.context,
                "reason_code": "SIMULATION_SELECTION_INFERENCE_IN_PROGRESS",
                "failure_stage": "SELECTION_INFERENCE",
                "started_at": entry.started_at,
                "elapsed_seconds": round(max(0.0, elapsed_seconds), 3),
                "timeout_seconds": self._selection_inference_timeout_seconds,
            },
        )

    def _selection_inference_timeout_error(
        self,
        entry: _SelectionInferenceInFlight,
        *,
        elapsed_seconds: float,
    ) -> ArtifactGenerationFailedError:
        return ArtifactGenerationFailedError(
            "simulation scheduler selection inference timed out; tick remains non-blocking",
            context={
                **entry.context,
                "reason_code": "SIMULATION_SELECTION_INFERENCE_TIMEOUT",
                "failure_stage": "SELECTION_INFERENCE",
                "started_at": entry.started_at,
                "elapsed_seconds": round(max(0.0, elapsed_seconds), 3),
                "timeout_seconds": self._selection_inference_timeout_seconds,
                "thread_isolated": True,
            },
        )

    def _selection_cache_key(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
    ) -> tuple[Any, ...]:
        return (
            binding.package_id,
            binding.manifest_sha256,
            binding.release_id,
            binding.release_hash,
            trade_date.isoformat(),
            data_source,
        )

    def _shared_selection_cache_keys(
        self,
        *,
        bindings: list[SimulationReleaseBinding],
        trade_date: date,
        data_source: str,
    ) -> set[tuple[Any, ...]]:
        """Share selection only when the same release fans out to multiple backends.

        Multiple strategies on the same backend keep the historical independent
        selection call contract until account-group slot semantics own that fanout.
        """

        backends_by_key: dict[tuple[Any, ...], set[str]] = {}
        for binding in bindings:
            key = self._selection_cache_key(
                binding=binding,
                trade_date=trade_date,
                data_source=data_source,
            )
            backend = binding.broker_backend.value if isinstance(binding.broker_backend, SimulationBrokerBackend) else str(binding.broker_backend)
            backends_by_key.setdefault(key, set()).add(backend)
        return {key for key, backends in backends_by_key.items() if len(backends) > 1}

    def _persist_strategy_performance(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
        local_persistence: LocalSimPersistenceResult | None = None,
    ) -> dict[str, Any]:
        if local_persistence is not None:
            latest = self.repository.get_simulation_daily_run(run.run_id)
            payload = latest.run_payload_json.get("strategy_performance")
            if not isinstance(payload, dict) or int(payload.get("local_sim_generation") or 0) != local_persistence.generation:
                raise DataUnavailableError("LocalSim performance projection generation does not match economic facts", context={"reason_code": "LOCALSIM_PERFORMANCE_GENERATION_CONFLICT", "run_id": run.run_id, "expected_generation": local_persistence.generation})
            return payload
        marks = self._performance_marks(context)
        if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM and (
            context.qmt_ledger_repository is None
            or self._has_miniqmt_position_reconciliation_adjustments(context)
        ):
            projection = self.performance_service.project_strategy(
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                initial_capital=float(binding.capital_allocation),
                cash=float(context.cash if context.cash is not None else binding.capital_allocation),
                frozen_cash=float(context.frozen_cash),
                realized_pnl=float(context.realized_pnl),
                positions=context.current_positions,
                marks=marks,
            )
        elif binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM and context.qmt_ledger_repository is not None:
            projection = self.performance_service.project_from_qmt_strategy_ledger(
                strategy_id=binding.strategy_id,
                repository=context.qmt_ledger_repository,
                marks=marks,
            )
        else:
            projection = self.performance_service.project_strategy(
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                initial_capital=float(binding.capital_allocation),
                cash=float(context.cash if context.cash is not None else binding.capital_allocation),
                frozen_cash=float(context.frozen_cash),
                realized_pnl=float(context.realized_pnl),
                positions=context.current_positions,
                marks=marks,
            )
        payload = projection.to_dict()
        if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM:
            latest_payload = self.repository.get_simulation_daily_run(run.run_id).run_payload_json
            payload = with_miniqmt_capacity_residual_observability(
                payload,
                self._miniqmt_capacity_residual_observability(latest_payload),
            )
        self.repository.update_simulation_daily_run(
            run.run_id,
            payload_patch={
                "strategy_performance": payload,
                "performance_projection": payload,
            },
        )
        return payload

    @staticmethod
    def _has_miniqmt_position_reconciliation_adjustments(context: SimulationRunContext) -> bool:
        diagnostics = context.context_diagnostics.get("miniqmt_broker_position_reconciliation")
        if not isinstance(diagnostics, dict):
            return False
        return bool(
            int(diagnostics.get("dropped_position_count") or 0) > 0
            or int(diagnostics.get("capped_position_count") or 0) > 0
        )

    def _persist_local_sim_execution_result(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        execution: SimulationExecutionResult,
        context: SimulationRunContext,
    ) -> LocalSimPersistenceResult | None:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM or execution.status != "SUBMITTED":
            return None
        try:
            snapshot = getattr(execution.broker_result, "execution_snapshot", None)
            if snapshot is None:
                raise DataUnavailableError(
                    "LocalSim submit returned no execution snapshot for durable persistence",
                    context={"run_id": run.run_id, "strategy_id": binding.strategy_id, "binding_id": binding.binding_id, "plan_id": execution.execution_plan.plan_id},
                )
            orders, fills, events, cash_entries = self._filter_local_sim_snapshot_by_plan(
                execution=execution,
                orders=tuple(getattr(snapshot, "orders", ()) or ()),
                fills=tuple(getattr(snapshot, "fills", ()) or ()),
                events=tuple(getattr(snapshot, "events", ()) or ()),
                cash_entries=tuple(getattr(snapshot, "cash_entries", ()) or ()),
            )
            positions = dict(getattr(snapshot, "positions", {}) or {})
            account = getattr(snapshot, "account", None)
            execution_states: tuple[LocalSimExecutionStateV1, ...] = ()
            if context.market_data_source == MinuteDataSource.TDX_REALTIME.value:
                exporter = getattr(context.local_broker, "export_execution_snapshot", None)
                if not callable(exporter):
                    raise DataUnavailableError("LocalSim realtime broker cannot export durable execution states", context={"reason_code": "LOCALSIM_DURABLE_STATE_EXPORT_UNSUPPORTED", "run_id": run.run_id, "plan_id": execution.execution_plan.plan_id})
                raw_snapshot = exporter(handles=tuple(getattr(execution.broker_result, "handles", ()) or ()))
                execution_states = tuple(raw_snapshot.get("execution_states") or ())
                self._validate_local_sim_execution_states(binding=binding, run=run, execution=execution, states=execution_states)
                self._validate_local_sim_snapshot_for_progress(run=run, execution=execution, orders=orders)
            else:
                self._validate_local_sim_snapshot_for_success(run=run, execution=execution, orders=orders, fills=fills, cash_entries=cash_entries)

            paper_repository = self._paper_repository_for_local_sim(binding=binding, run=run, context=context)
            self._replay_pending_local_sim_projection(run_id=run.run_id, paper_repository=paper_repository)
            current_states = {state.state_id: state for state in self.repository.list_local_sim_execution_states(run.run_id)}
            if (
                execution_states
                and not fills
                and not events
                and not cash_entries
                and all(
                    state.state_id in current_states
                    and current_states[state.state_id].sequence == state.sequence
                    and current_states[state.state_id].state_hash == state.state_hash
                    for state in execution_states
                )
            ):
                return self._local_sim_existing_projection_result(run_id=run.run_id)
            snapshot_time = self._local_sim_snapshot_time(
                fills=fills,
                events=events,
                run=run,
                local_broker=context.local_broker,
                market_data_source=context.market_data_source,
            )
            marks, mark_records = self._local_sim_position_marks(
                positions=positions, context=context, execution=execution, snapshot_time=snapshot_time
            )
            cash = float(getattr(account, "cash")) if account is not None else self._cash_after_local_sim(cash_entries, context)
            market_value = sum(int(position.quantity) * marks[position.symbol] for position in positions.values())
            account_snapshot = AccountSnapshot(
                portfolio_id=str(context.portfolio_id or execution.execution_plan.portfolio_id),
                cash=cash, market_value=market_value, nav=cash + market_value, snapshot_time=snapshot_time,
            )
            cash_fit_residual = self._local_sim_cash_fit_residual_payload(run)
            active_states = tuple(state for state in execution_states if not state.is_terminal)
            residual_states = tuple(state for state in execution_states if state.runtime_status == LocalSimExecutionRuntimeStatus.EXPIRED_WITH_RESIDUAL)
            nonfilled_terminal_states = tuple(state for state in execution_states if state.is_terminal and state.runtime_status != LocalSimExecutionRuntimeStatus.FILLED)
            terminal = not active_states
            terminal_failure = bool(cash_fit_residual or residual_states or nonfilled_terminal_states)
            if active_states:
                final_event_type, final_event_message = "RUN_INTRADAY_PROGRESS", "simulation runtime LocalSim minute progress projected"
                final_paper_status, final_status, persistence_status = RunStatus.RUNNING, SimulationDailyRunStatus.INTRADAY_RUNNING, "INTRADAY_PERSISTED"
            elif terminal_failure:
                final_event_type, final_event_message = "RUN_TERMINATED_WITH_RESIDUAL", "simulation runtime LocalSim execution terminalized with explicit residual"
                final_paper_status, final_status = RunStatus.FAILED, SimulationDailyRunStatus.FAILED_TERMINAL
                persistence_status = "PERSISTED_WITH_CAPACITY_RESIDUAL" if cash_fit_residual else "PERSISTED_WITH_RESIDUAL"
            else:
                final_event_type, final_event_message = "RUN_SUCCEEDED", "simulation runtime LocalSim terminal execution projected to Paper v2"
                final_paper_status, final_status, persistence_status = RunStatus.SUCCEEDED, SimulationDailyRunStatus.SUCCEEDED, "PERSISTED"

            final_persistence_payload = {
                "schema_version": "local_sim_persistence_v2", "status": persistence_status,
                "paper_v2_run_id": run.run_id, "order_count": len(orders), "fill_count": len(fills),
                "order_event_count": len(events), "cash_ledger_count": len(cash_entries),
                "position_count": len(positions), "snapshot_time": snapshot_time.isoformat(),
                "cash": cash, "nav": account_snapshot.nav, "terminal": terminal,
                "execution_state_count": len(execution_states), "active_state_count": len(active_states),
                "residual_state_count": len(residual_states),
            }
            payload_patch: dict[str, Any] = {
                "local_sim_persistence": {**final_persistence_payload, "status": "PROJECTION_PENDING"},
                "last_stage": "LOCAL_SIM_ECONOMIC_COMMITTED",
            }
            if execution_states:
                payload_patch["local_sim_durable_minute_loop"] = {
                    "schema_version": "local_sim_durable_minute_loop_v1", "state_count": len(execution_states),
                    "active_state_count": len(active_states), "terminal": terminal,
                }
            if terminal_failure:
                payload_patch["local_sim_capacity_residual_terminalization"] = {
                    "schema_version": "localsim_capacity_residual_terminalization_v1",
                    "reason": "cash_fit_skipped_non_executable_buy_residual" if cash_fit_residual else "execution_schedule_residual_at_close",
                    "status": final_status.value,
                    "skipped_buy_count": int((cash_fit_residual or {}).get("skipped_buy_count") or 0),
                    "prepared_intent_count": int((cash_fit_residual or {}).get("prepared_intent_count") or 0),
                    "residual_state_ids": [state.state_id for state in residual_states],
                    "terminalized_at": datetime.now(UTC).isoformat(),
                }
            economic_facts = self._local_sim_economic_facts(
                run=run, execution=execution, orders=orders, fills=fills, events=events,
                cash_entries=cash_entries, states=execution_states, positions=positions,
                marks=mark_records, account_snapshot=account_snapshot,
            )
            economic_hash = canonical_json_sha256(economic_facts)
            projection_payload = self._local_sim_projection_payload(
                binding=binding, run=run, execution=execution, context=context,
                positions=positions, marks=mark_records, account_snapshot=account_snapshot,
                orders=orders, fills=fills, cash_entries=cash_entries,
                active_states=active_states, residual_states=residual_states,
                nonfilled_terminal_states=nonfilled_terminal_states, cash_fit_residual=cash_fit_residual,
                terminal=terminal, terminal_failure=terminal_failure, final_status=final_status,
                final_paper_status=final_paper_status, final_event_type=final_event_type,
                final_event_message=final_event_message, final_persistence_payload=final_persistence_payload,
                economic_hash=economic_hash,
            )
            expected_versions = {
                state.state_id: ((current_states[state.state_id].sequence, current_states[state.state_id].state_hash) if state.state_id in current_states else None)
                for state in execution_states
            }
            with self.repository.local_sim_economic_transaction_scope():
                with paper_repository.local_sim_economic_transaction(run.run_id) as connection:
                    self._ensure_local_sim_paper_run(repository=paper_repository, run=run, context=context)
                    for order in orders:
                        paper_repository.save_order(run.run_id, order)
                    for fill in fills:
                        paper_repository.save_fill(run.run_id, fill)
                    for event in events:
                        paper_repository.save_order_event(run.run_id, event)
                    for entry in cash_entries:
                        paper_repository.save_cash_entry(run.run_id, entry)
                    receipt, outbox, created = self.repository.stage_local_sim_economic_commit(
                        connection=connection, run_id=run.run_id, binding_id=binding.binding_id,
                        trade_date=run.trade_date, plan_id=execution.execution_plan.plan_id,
                        states=execution_states, expected_versions=expected_versions,
                        economic_facts=economic_facts, projection_payload=projection_payload,
                        status=SimulationDailyRunStatus.INTRADAY_RUNNING, payload_patch=payload_patch,
                        payload_unset=("submit_failure", "local_sim_retry_diagnostics", *(("local_sim_synchronous_terminal",) if execution_states else ())),
                    )
                    if created:
                        paper_repository.save_run_event(
                            run_id=run.run_id, event_type="RUN_ECONOMIC_COMMITTED",
                            message="simulation runtime LocalSim economic facts committed; projection outbox pending",
                            context={"source": "simulation_runtime_local_sim", "simulation_run_id": run.run_id,
                                     "execution_plan_id": execution.execution_plan.plan_id, "receipt_id": receipt.receipt_id,
                                     "outbox_id": outbox.outbox_id, "generation": receipt.generation,
                                     "economic_hash": receipt.economic_hash},
                        )
            self.repository.readback_local_sim_economic_commit(run_id=run.run_id, receipt=receipt, outbox=outbox)
            paper_repository.readback_local_sim_economic_facts(
                run_id=run.run_id, order_ids={str(item.order_id) for item in orders},
                fill_ids={str(item.fill_id) for item in fills},
                order_event_ids={str(item.event_id) for item in events},
                cash_fill_ids={str(item.fill_id) for item in cash_entries},
            )
            projected_run, performance_payload = self._project_local_sim_outbox(run_id=run.run_id, paper_repository=paper_repository)
            if not isinstance(projected_run.run_payload_json.get("local_sim_persistence"), dict):
                raise DataUnavailableError("LocalSim projected persistence receipt is missing", context={"reason_code": "LOCALSIM_PERSISTENCE_RECEIPT_MISSING", "run_id": run.run_id})
            return LocalSimPersistenceResult(
                payload={"order_count": len(orders), "fill_count": len(fills), "cash_ledger_count": len(cash_entries),
                         "position_count": len(positions), "cash": cash, "nav": account_snapshot.nav,
                         "terminal": terminal, "active_state_count": len(active_states),
                         "residual_state_count": len(residual_states)},
                positions=positions, marks=marks, cash=cash, economic_receipt_id=receipt.receipt_id,
                outbox_id=outbox.outbox_id, generation=receipt.generation, performance_payload=performance_payload,
            )
        except Exception as exc:
            if not isinstance(exc, DataUnavailableError):
                exc = DataUnavailableError(
                    "LocalSim execution side effects could not be persisted durably",
                    context={"run_id": run.run_id, "strategy_id": binding.strategy_id, "binding_id": binding.binding_id,
                             "plan_id": execution.execution_plan.plan_id, "cause": str(exc)},
                )
            failure_context = dict(getattr(exc, "context", None) or {})
            reason_code = str(failure_context.get("reason_code") or "")
            failure_stage = self._local_sim_persistence_failure_stage(exc)
            if reason_code in {
                "LOCALSIM_PROJECTION_NON_RETRYABLE",
                "LOCALSIM_PROJECTION_RETRY_EXHAUSTED",
                "LOCALSIM_PROJECTION_READBACK_RETRY_EXHAUSTED",
            }:
                self.repository.update_simulation_daily_run(
                    run.run_id,
                    status=SimulationDailyRunStatus.FAILED_TERMINAL,
                    payload_patch={
                        "last_stage": SimulationDailyRunStatus.FAILED_TERMINAL.value,
                        "submit_failure": {
                            "stage": failure_stage,
                            "outer_stage": failure_stage,
                            "type": type(exc).__name__,
                            "message": str(exc),
                            "context": failure_context,
                        },
                    },
                )
            else:
                self.orchestrator.mark_submit_failure(
                    run=run,
                    stage=failure_stage,
                    exc=exc,
                )
            raise exc
    @staticmethod
    def _local_sim_json_value(value: Any) -> Any:
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, Mapping):
            return {str(key): SimulationLifecycleScheduler._local_sim_json_value(item) for key, item in sorted(value.items(), key=lambda row: str(row[0]))}
        if isinstance(value, (list, tuple)):
            return [SimulationLifecycleScheduler._local_sim_json_value(item) for item in value]
        return value

    @staticmethod
    def _local_sim_fact_payload(item: Any, *, fact_type: str) -> dict[str, Any]:
        dump = getattr(item, "model_dump", None)
        if callable(dump):
            raw = dump(mode="json", exclude={"created_at", "updated_at"})
        elif is_dataclass(item):
            raw = asdict(item)
        else:
            raise DataUnavailableError("LocalSim economic fact cannot be serialized canonically", context={"reason_code": "LOCALSIM_ECONOMIC_FACT_SCHEMA_INVALID", "fact_type": fact_type, "python_type": type(item).__name__})
        payload = SimulationLifecycleScheduler._local_sim_json_value(raw)
        if not isinstance(payload, dict):
            raise DataUnavailableError("LocalSim economic fact canonical payload must be an object", context={"reason_code": "LOCALSIM_ECONOMIC_FACT_SCHEMA_INVALID", "fact_type": fact_type})
        return payload

    @classmethod
    def _local_sim_hashed_fact_map(cls, items: tuple[Any, ...], *, identity_field: str, fact_type: str) -> dict[str, str]:
        hashed: dict[str, str] = {}
        for item in items:
            identity = str(getattr(item, identity_field, "") or "").strip()
            if not identity or identity in hashed:
                raise DataUnavailableError("LocalSim economic fact identity is missing or duplicated", context={"reason_code": "LOCALSIM_ECONOMIC_FACT_IDENTITY_INVALID", "fact_type": fact_type, "identity": identity or None})
            hashed[identity] = canonical_json_sha256(cls._local_sim_fact_payload(item, fact_type=fact_type))
        return dict(sorted(hashed.items()))

    @classmethod
    def _local_sim_economic_facts(cls, *, run: SimulationDailyRun, execution: SimulationExecutionResult, orders: tuple[Any, ...], fills: tuple[Any, ...], events: tuple[Any, ...], cash_entries: tuple[Any, ...], states: tuple[LocalSimExecutionStateV1, ...], positions: dict[str, PositionLot], marks: dict[str, LocalSimMarketMarkV1], account_snapshot: AccountSnapshot) -> dict[str, Any]:
        return {
            "schema_version": "local_sim_economic_facts_v1", "run_id": run.run_id,
            "binding_id": run.binding_id, "trade_date": run.trade_date.isoformat(),
            "plan_id": execution.execution_plan.plan_id,
            "order_hashes": cls._local_sim_hashed_fact_map(orders, identity_field="order_id", fact_type="order"),
            "fill_hashes": cls._local_sim_hashed_fact_map(fills, identity_field="fill_id", fact_type="fill"),
            "order_event_hashes": cls._local_sim_hashed_fact_map(events, identity_field="event_id", fact_type="order_event"),
            "cash_entry_hashes": cls._local_sim_hashed_fact_map(cash_entries, identity_field="fill_id", fact_type="cash_entry"),
            "state_hashes": {state.state_id: state.state_hash for state in sorted(states, key=lambda item: item.state_id)},
            "position_hashes": {symbol: canonical_json_sha256(cls._local_sim_fact_payload(position, fact_type="position")) for symbol, position in sorted(positions.items())},
            "mark_hashes": {symbol: mark.mark_hash for symbol, mark in sorted(marks.items())},
            "account_snapshot_hash": canonical_json_sha256(cls._local_sim_fact_payload(account_snapshot, fact_type="account_snapshot")),
        }

    @staticmethod
    def _local_sim_projection_payload(
        *, binding: SimulationReleaseBinding, run: SimulationDailyRun,
        execution: SimulationExecutionResult, context: SimulationRunContext,
        positions: dict[str, PositionLot], marks: dict[str, LocalSimMarketMarkV1],
        account_snapshot: AccountSnapshot, orders: tuple[Any, ...], fills: tuple[Any, ...],
        cash_entries: tuple[Any, ...], active_states: tuple[LocalSimExecutionStateV1, ...],
        residual_states: tuple[LocalSimExecutionStateV1, ...],
        nonfilled_terminal_states: tuple[LocalSimExecutionStateV1, ...],
        cash_fit_residual: dict[str, Any] | None, terminal: bool, terminal_failure: bool,
        final_status: SimulationDailyRunStatus, final_paper_status: RunStatus,
        final_event_type: str, final_event_message: str,
        final_persistence_payload: dict[str, Any], economic_hash: str,
    ) -> dict[str, Any]:
        return {
            "schema_version": "local_sim_projection_payload_v1", "run_id": run.run_id,
            "binding_id": binding.binding_id, "strategy_id": binding.strategy_id,
            "plan_id": execution.execution_plan.plan_id, "trade_date": run.trade_date.isoformat(),
            "portfolio_id": account_snapshot.portfolio_id, "initial_capital": float(binding.capital_allocation),
            "realized_pnl": float(context.realized_pnl),
            "positions": [item.model_dump(mode="json") for _, item in sorted(positions.items())],
            "marks": [item.model_dump(mode="json") for _, item in sorted(marks.items())],
            "account_snapshot": account_snapshot.model_dump(mode="json"),
            "snapshot_metadata": {"source": "simulation_runtime_local_sim", "simulation_run_id": run.run_id,
                                  "execution_plan_id": execution.execution_plan.plan_id, "order_count": len(orders),
                                  "fill_count": len(fills), "cash_ledger_count": len(cash_entries),
                                  "position_count": len(positions), "terminal": terminal},
            "final_simulation_status": final_status.value, "final_paper_status": final_paper_status.value,
            "final_event_type": final_event_type, "final_event_message": final_event_message,
            "final_event_context": {"source": "simulation_runtime_local_sim", "simulation_run_id": run.run_id,
                                    "execution_plan_id": execution.execution_plan.plan_id, "order_count": len(orders),
                                    "fill_count": len(fills), "cash_ledger_count": len(cash_entries),
                                    "position_count": len(positions), "snapshot_time": account_snapshot.snapshot_time.isoformat(),
                                    "local_sim_cash_fit": cash_fit_residual, "terminal": terminal,
                                    "active_state_ids": [item.state_id for item in active_states],
                                    "residual_state_ids": [item.state_id for item in residual_states]},
            "paper_error": ({"code": "LOCALSIM_CAPACITY_RESIDUAL_SKIPPED" if cash_fit_residual else "LOCALSIM_EXECUTION_TERMINATED_WITH_RESIDUAL",
                             "message": "LocalSim skipped non-executable BUY residual after cash-fit planning" if cash_fit_residual else "LocalSim closed with explicit unfilled execution residual",
                             "context": {"local_sim_cash_fit": cash_fit_residual, "states": [item.model_dump(mode="json") for item in nonfilled_terminal_states]}} if terminal_failure else None),
            "local_sim_persistence": final_persistence_payload, "economic_hash": economic_hash,
            "tca_generation": {"schema_version": "local_sim_tca_generation_v1",
                               "execution_plan_id": execution.execution_plan.plan_id,
                               "execution_plan_hash": execution.execution_plan.plan_hash,
                               "economic_hash": economic_hash},
        }

    def _local_sim_existing_projection_result(self, *, run_id: str) -> LocalSimPersistenceResult:
        run = self.repository.get_simulation_daily_run(run_id)
        try:
            outbox = LocalSimProjectionOutboxV1.model_validate(run.run_payload_json.get("local_sim_projection_outbox_v1"))
            if outbox.status != LocalSimProjectionOutboxStatus.PROJECTED:
                raise ValueError("projection outbox is not projected")
            payload = outbox.projection_payload
            positions = {item.symbol: item for item in (PositionLot.model_validate(raw) for raw in payload.get("positions") or [])}
            marks = {item.symbol: item.price for item in (LocalSimMarketMarkV1.model_validate(raw) for raw in payload.get("marks") or [])}
            account = AccountSnapshot.model_validate(payload.get("account_snapshot"))
            performance = run.run_payload_json["strategy_performance"]
            persistence = run.run_payload_json["local_sim_persistence"]
        except Exception as exc:
            raise DataUnavailableError("LocalSim duplicate event cannot rebuild the projected generation", context={"reason_code": "LOCALSIM_DUPLICATE_PROJECTION_READBACK_FAILED", "run_id": run_id}) from exc
        if not isinstance(performance, dict) or not isinstance(persistence, dict):
            raise DataUnavailableError("LocalSim duplicate event is missing projected receipts", context={"reason_code": "LOCALSIM_DUPLICATE_PROJECTION_READBACK_FAILED", "run_id": run_id})
        required_counts = (
            "order_count", "fill_count", "cash_ledger_count", "position_count",
            "active_state_count", "residual_state_count", "terminal",
        )
        missing = [key for key in required_counts if key not in persistence]
        if missing:
            raise DataUnavailableError("LocalSim duplicate projection receipt is incomplete", context={"reason_code": "LOCALSIM_DUPLICATE_PROJECTION_READBACK_FAILED", "run_id": run_id, "missing_fields": missing})
        return LocalSimPersistenceResult(
            payload={
                "order_count": int(persistence["order_count"]),
                "fill_count": int(persistence["fill_count"]),
                "cash_ledger_count": int(persistence["cash_ledger_count"]),
                "position_count": int(persistence["position_count"]),
                "cash": float(account.cash), "nav": float(account.nav),
                "terminal": bool(persistence["terminal"]),
                "active_state_count": int(persistence["active_state_count"]),
                "residual_state_count": int(persistence["residual_state_count"]),
            },
            positions=positions, marks=marks, cash=float(account.cash),
            economic_receipt_id=outbox.receipt_id, outbox_id=outbox.outbox_id,
            generation=outbox.generation, performance_payload=performance,
        )

    def _replay_pending_local_sim_projection(self, *, run_id: str, paper_repository: Any) -> None:
        run = self.repository.get_simulation_daily_run(run_id)
        raw = run.run_payload_json.get("local_sim_projection_outbox_v1")
        if raw is None:
            return
        try:
            outbox = LocalSimProjectionOutboxV1.model_validate(raw)
        except Exception as exc:
            raise DataUnavailableError("LocalSim projection outbox cannot be recovered", context={"reason_code": "LOCALSIM_PROJECTION_OUTBOX_SCHEMA_INVALID", "run_id": run_id}) from exc
        if outbox.status in {LocalSimProjectionOutboxStatus.PENDING, LocalSimProjectionOutboxStatus.PROJECTION_RETRYABLE} or run.run_payload_json.get("local_sim_projection_readback_failure"):
            self._project_local_sim_outbox(run_id=run_id, paper_repository=paper_repository)

    def _project_local_sim_outbox(self, *, run_id: str, paper_repository: Any) -> tuple[SimulationDailyRun, dict[str, Any]]:
        run = self.repository.get_simulation_daily_run(run_id)
        terminal_failure = run.run_payload_json.get("local_sim_projection_terminal_failure")
        if isinstance(terminal_failure, dict):
            terminal_error = dict(terminal_failure.get("error") or {})
            raise DataUnavailableError(
                "LocalSim projection is terminal and cannot be retried automatically",
                context={
                    "reason_code": str(
                        terminal_error.get("reason_code")
                        or "LOCALSIM_PROJECTION_NON_RETRYABLE"
                    ),
                    "run_id": run_id,
                    "outbox_id": terminal_failure.get("outbox_id"),
                    "attempt_count": terminal_failure.get("attempt_count"),
                    "cause": terminal_error.get("message"),
                },
            )
        raw = run.run_payload_json.get("local_sim_projection_outbox_v1")
        if raw is None:
            raise DataUnavailableError("LocalSim economic commit has no projection outbox", context={"reason_code": "LOCALSIM_PROJECTION_OUTBOX_MISSING", "run_id": run_id})
        try:
            outbox = LocalSimProjectionOutboxV1.model_validate(raw)
        except Exception as exc:
            raise DataUnavailableError("LocalSim projection outbox cannot be read", context={"reason_code": "LOCALSIM_PROJECTION_OUTBOX_SCHEMA_INVALID", "run_id": run_id}) from exc
        performance = run.run_payload_json.get("strategy_performance")
        if outbox.status == LocalSimProjectionOutboxStatus.PROJECTED:
            if not isinstance(performance, dict) or int(performance.get("local_sim_generation") or 0) != outbox.generation:
                raise DataUnavailableError("LocalSim projected outbox has no matching performance generation", context={"reason_code": "LOCALSIM_PERFORMANCE_GENERATION_CONFLICT", "run_id": run_id})
            readback_failure = run.run_payload_json.get("local_sim_projection_readback_failure")
            if readback_failure:
                if not isinstance(readback_failure, dict):
                    raise DataUnavailableError(
                        "LocalSim projection readback failure receipt is invalid",
                        context={
                            "reason_code": "LOCALSIM_PROJECTION_READBACK_SCHEMA_INVALID",
                            "run_id": run_id,
                        },
                    )
                previous_attempts = int(readback_failure.get("attempt_count") or 0)
                if previous_attempts >= _LOCALSIM_PROJECTION_MAX_ATTEMPTS:
                    raise DataUnavailableError(
                        "LocalSim projection readback exhausted its automatic retry budget",
                        context={
                            "reason_code": "LOCALSIM_PROJECTION_READBACK_RETRY_EXHAUSTED",
                            "run_id": run_id,
                            "outbox_id": outbox.outbox_id,
                            "attempt_count": previous_attempts,
                        },
                    )
                raw_receipts = run.run_payload_json.get("local_sim_projection_receipts_v1")
                if not isinstance(raw_receipts, dict):
                    raise DataUnavailableError("LocalSim projection readback recovery has no receipt map", context={"reason_code": "LOCALSIM_PROJECTION_RECEIPT_MISSING", "run_id": run_id})
                receipt = next((LocalSimProjectionReceiptV1.model_validate(item) for item in raw_receipts.values() if item.get("outbox_id") == outbox.outbox_id), None)
                if receipt is None:
                    raise DataUnavailableError("LocalSim projection readback recovery has no matching receipt", context={"reason_code": "LOCALSIM_PROJECTION_RECEIPT_MISSING", "run_id": run_id})
                payload = outbox.projection_payload
                snapshot = AccountSnapshot.model_validate(payload.get("account_snapshot"))
                trade_date_value = date.fromisoformat(str(payload.get("trade_date")))
                final_status = SimulationDailyRunStatus(str(payload.get("final_simulation_status")))
                try:
                    self.repository.readback_local_sim_projection_commit(run_id=run_id, receipt=receipt)
                    paper_repository.readback_local_sim_projection(
                        run_id=run_id, portfolio_id=snapshot.portfolio_id, trade_date=trade_date_value,
                        outbox_id=outbox.outbox_id, generation=outbox.generation,
                        expected_position_count=len(payload.get("positions") or []),
                    )
                except Exception as exc:
                    attempt_count = previous_attempts + 1
                    error = {
                        "reason_code": "LOCALSIM_PROJECTION_READBACK_RETRYABLE",
                        "type": type(exc).__name__,
                        "message": str(exc),
                        "outbox_id": outbox.outbox_id,
                        "generation": outbox.generation,
                        "attempt_count": attempt_count,
                    }
                    self.repository.mark_local_sim_projection_readback_retryable(
                        run_id=run_id,
                        outbox_id=outbox.outbox_id,
                        error=error,
                    )
                    if attempt_count >= _LOCALSIM_PROJECTION_MAX_ATTEMPTS:
                        self.repository.update_simulation_daily_run(
                            run_id,
                            status=SimulationDailyRunStatus.FAILED_TERMINAL,
                            payload_patch={
                                "local_sim_projection_readback_terminal_failure": error,
                                "last_stage": SimulationDailyRunStatus.FAILED_TERMINAL.value,
                            },
                        )
                    reason_code = (
                        "LOCALSIM_PROJECTION_READBACK_RETRY_EXHAUSTED"
                        if attempt_count >= _LOCALSIM_PROJECTION_MAX_ATTEMPTS
                        else "LOCALSIM_PROJECTION_READBACK_RETRYABLE"
                    )
                    raise DataUnavailableError(
                        "LocalSim projection readback must be retried",
                        context={
                            "reason_code": reason_code,
                            "run_id": run_id,
                            "outbox_id": outbox.outbox_id,
                            "attempt_count": attempt_count,
                            "cause": str(exc),
                        },
                    ) from exc
                run = self.repository.clear_local_sim_projection_readback_failure(run_id=run_id, outbox_id=outbox.outbox_id, final_status=final_status)
            return run, performance

        payload = outbox.projection_payload
        try:
            positions = {item.symbol: item for item in (PositionLot.model_validate(raw_item) for raw_item in payload.get("positions") or [])}
            mark_records = {item.symbol: item for item in (LocalSimMarketMarkV1.model_validate(raw_item) for raw_item in payload.get("marks") or [])}
            account_snapshot = AccountSnapshot.model_validate(payload.get("account_snapshot"))
            final_status = SimulationDailyRunStatus(str(payload.get("final_simulation_status")))
            final_paper_status = RunStatus(str(payload.get("final_paper_status")))
            projection_trade_date = date.fromisoformat(str(payload.get("trade_date")))
        except Exception as exc:
            raise DataUnavailableError("LocalSim projection payload failed schema validation", context={"reason_code": "LOCALSIM_PROJECTION_PAYLOAD_SCHEMA_INVALID", "run_id": run_id}) from exc
        if payload.get("economic_hash") != outbox.economic_hash:
            raise DataUnavailableError("LocalSim projection payload economic hash does not match outbox", context={"reason_code": "LOCALSIM_PROJECTION_ECONOMIC_HASH_CONFLICT", "run_id": run_id})
        strategy_id = str(payload.get("strategy_id") or "").strip()
        if not strategy_id or "initial_capital" not in payload or "realized_pnl" not in payload:
            raise DataUnavailableError("LocalSim projection payload is missing performance identity", context={"reason_code": "LOCALSIM_PROJECTION_PAYLOAD_SCHEMA_INVALID", "run_id": run_id})
        marks = {symbol: mark.price for symbol, mark in mark_records.items()}
        performance = self.performance_service.project_strategy(
            strategy_id=strategy_id, broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            initial_capital=float(payload["initial_capital"]), cash=float(account_snapshot.cash),
            frozen_cash=0.0, realized_pnl=float(payload["realized_pnl"]),
            positions=positions, marks=marks,
        ).to_dict()
        performance.update({"local_sim_generation": outbox.generation, "local_sim_outbox_id": outbox.outbox_id,
                            "local_sim_economic_hash": outbox.economic_hash,
                            "tca_generation": {**dict(payload.get("tca_generation") or {}), "generation": outbox.generation}})
        snapshot_metadata = {**dict(payload.get("snapshot_metadata") or {}), "local_sim_generation": outbox.generation,
                             "local_sim_outbox_id": outbox.outbox_id, "local_sim_economic_hash": outbox.economic_hash,
                             "projection_payload_hash": outbox.projection_payload_hash}
        projection_result = {
            "schema_version": "local_sim_projection_result_v1", "outbox_id": outbox.outbox_id,
            "generation": outbox.generation, "economic_hash": outbox.economic_hash,
            "position_hashes": {symbol: canonical_json_sha256(self._local_sim_fact_payload(item, fact_type="position")) for symbol, item in sorted(positions.items())},
            "mark_hashes": {symbol: item.mark_hash for symbol, item in sorted(mark_records.items())},
            "account_snapshot_hash": canonical_json_sha256(self._local_sim_fact_payload(account_snapshot, fact_type="account_snapshot")),
            "performance_hash": canonical_json_sha256(performance),
        }
        projection_committed = False
        try:
            with self.repository.local_sim_economic_transaction_scope():
                with paper_repository.local_sim_economic_transaction(run_id) as connection:
                    paper_repository.save_positions(run_id=run_id, trade_date=projection_trade_date, positions=list(positions.values()), prices=marks)
                    paper_repository.save_daily_snapshot(run_id=run_id, trade_date=projection_trade_date, snapshot=account_snapshot, metadata=snapshot_metadata)
                    paper_repository.save_run_event(run_id=run_id, event_type=str(payload.get("final_event_type")), message=str(payload.get("final_event_message")), context={**dict(payload.get("final_event_context") or {}), "local_sim_generation": outbox.generation, "local_sim_outbox_id": outbox.outbox_id, "local_sim_economic_hash": outbox.economic_hash})
                    paper_repository.update_run_status(paper_repository.get_run(run_id), final_paper_status, error=payload.get("paper_error"))
                    receipt = self.repository.stage_local_sim_projection_commit(
                        connection=connection, run_id=run_id, outbox_id=outbox.outbox_id,
                        generation=outbox.generation, final_status=final_status, projection_result=projection_result,
                        payload_patch={"strategy_performance": performance, "performance_projection": performance,
                                       "local_sim_persistence": dict(payload.get("local_sim_persistence") or {}),
                                       "local_sim_projection_generation": {"schema_version": "local_sim_projection_generation_v1", "generation": outbox.generation, "outbox_id": outbox.outbox_id, "economic_hash": outbox.economic_hash},
                                       "last_stage": final_status.value},
                        payload_unset=("submit_failure", "local_sim_retry_diagnostics"),
                    )
            projection_committed = True
            projected = self.repository.readback_local_sim_projection_commit(run_id=run_id, receipt=receipt)
            paper_repository.readback_local_sim_projection(
                run_id=run_id, portfolio_id=account_snapshot.portfolio_id, trade_date=projection_trade_date,
                outbox_id=outbox.outbox_id, generation=outbox.generation, expected_position_count=len(positions),
            )
            return projected, performance
        except Exception as exc:
            previous_readback_failure = run.run_payload_json.get(
                "local_sim_projection_readback_failure"
            )
            previous_readback_attempts = (
                int(previous_readback_failure.get("attempt_count") or 0)
                if isinstance(previous_readback_failure, dict)
                else 0
            )
            attempt_count = (
                previous_readback_attempts + 1
                if projection_committed
                else outbox.attempt_count + 1
            )
            retryable = self._local_sim_projection_error_is_retryable(exc)
            if projection_committed:
                reason_code = "LOCALSIM_PROJECTION_READBACK_RETRYABLE"
            elif not retryable:
                reason_code = "LOCALSIM_PROJECTION_NON_RETRYABLE"
            elif attempt_count >= _LOCALSIM_PROJECTION_MAX_ATTEMPTS:
                reason_code = "LOCALSIM_PROJECTION_RETRY_EXHAUSTED"
            else:
                reason_code = "LOCALSIM_PROJECTION_RETRYABLE"
            error = {
                "reason_code": reason_code,
                "type": type(exc).__name__,
                "message": str(exc),
                "outbox_id": outbox.outbox_id,
                "generation": outbox.generation,
                "attempt_count": attempt_count,
                "max_attempts": _LOCALSIM_PROJECTION_MAX_ATTEMPTS,
            }
            try:
                if projection_committed:
                    self.repository.mark_local_sim_projection_readback_retryable(run_id=run_id, outbox_id=outbox.outbox_id, error=error)
                    if attempt_count >= _LOCALSIM_PROJECTION_MAX_ATTEMPTS:
                        self.repository.update_simulation_daily_run(
                            run_id,
                            status=SimulationDailyRunStatus.FAILED_TERMINAL,
                            payload_patch={
                                "local_sim_projection_readback_terminal_failure": error,
                                "last_stage": SimulationDailyRunStatus.FAILED_TERMINAL.value,
                            },
                        )
                elif reason_code in {
                    "LOCALSIM_PROJECTION_NON_RETRYABLE",
                    "LOCALSIM_PROJECTION_RETRY_EXHAUSTED",
                }:
                    self.repository.mark_local_sim_projection_terminal(
                        run_id=run_id,
                        outbox_id=outbox.outbox_id,
                        error=error,
                    )
                else:
                    self.repository.mark_local_sim_projection_retryable(run_id=run_id, outbox_id=outbox.outbox_id, error=error)
            except Exception as persistence_exc:
                raise DataUnavailableError("LocalSim projection failed and retry state could not be persisted", context={"reason_code": "LOCALSIM_PROJECTION_FAILURE_PERSISTENCE_FAILED", "run_id": run_id, "outbox_id": outbox.outbox_id, "projection_error": str(exc), "persistence_error": str(persistence_exc)}) from persistence_exc
            if projection_committed and attempt_count >= _LOCALSIM_PROJECTION_MAX_ATTEMPTS:
                reason_code = "LOCALSIM_PROJECTION_READBACK_RETRY_EXHAUSTED"
            message = (
                "LocalSim economic facts committed but projection cannot be retried"
                if reason_code == "LOCALSIM_PROJECTION_NON_RETRYABLE"
                else "LocalSim economic facts committed but projection retry budget is exhausted"
                if reason_code in {
                    "LOCALSIM_PROJECTION_RETRY_EXHAUSTED",
                    "LOCALSIM_PROJECTION_READBACK_RETRY_EXHAUSTED",
                }
                else "LocalSim economic facts committed but projection must be retried"
            )
            raise DataUnavailableError(
                message,
                context={
                    "reason_code": reason_code,
                    "run_id": run_id,
                    "outbox_id": outbox.outbox_id,
                    "generation": outbox.generation,
                    "attempt_count": attempt_count,
                    "max_attempts": _LOCALSIM_PROJECTION_MAX_ATTEMPTS,
                    "cause": str(exc),
                },
            ) from exc

    @staticmethod
    def _local_sim_projection_error_is_retryable(exc: BaseException) -> bool:
        current: BaseException | None = exc
        seen: set[int] = set()
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            if isinstance(
                current,
                (
                    SessionLockTimeoutError,
                    psycopg2.OperationalError,
                    psycopg2.InterfaceError,
                ),
            ):
                return True
            if str(getattr(current, "pgcode", "") or "") in _LOCALSIM_PROJECTION_RETRYABLE_PG_CODES:
                return True
            current = current.__cause__ or current.__context__
        return False

    @staticmethod
    def _filter_local_sim_snapshot_by_plan(
        *,
        execution: SimulationExecutionResult,
        orders: tuple[Any, ...],
        fills: tuple[Any, ...],
        events: tuple[Any, ...],
        cash_entries: tuple[Any, ...],
    ) -> tuple[tuple[Any, ...], tuple[Any, ...], tuple[Any, ...], tuple[Any, ...]]:
        plan_intent_ids = {intent.intent_id for intent in execution.execution_plan.intents}
        selected_orders = tuple(order for order in orders if getattr(order, "intent_id", None) in plan_intent_ids)
        selected_order_ids = {getattr(order, "order_id", None) for order in selected_orders}
        selected_fills = tuple(fill for fill in fills if getattr(fill, "order_id", None) in selected_order_ids)
        selected_fill_ids = {getattr(fill, "fill_id", None) for fill in selected_fills}
        selected_events = tuple(
            event
            for event in events
            if getattr(event, "order_id", None) in selected_order_ids
            and (getattr(event, "fill", None) is None or getattr(getattr(event, "fill", None), "fill_id", None) in selected_fill_ids)
        )
        selected_cash_entries = tuple(
            entry for entry in cash_entries if getattr(entry, "fill_id", None) in selected_fill_ids
        )
        return selected_orders, selected_fills, selected_events, selected_cash_entries

    @staticmethod
    def _local_sim_cash_fit_residual_payload(run: SimulationDailyRun) -> dict[str, Any] | None:
        payload = run.run_payload_json.get("local_sim_cash_fit")
        if not isinstance(payload, dict) or payload.get("status") != "CAPACITY_RESIDUAL_SKIPPED":
            return None
        return payload

    def _paper_repository_for_local_sim(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> Any:
        if context.paper_repository is not None:
            return context.paper_repository
        if isinstance(self.repository, InMemorySimulationRuntimeRepository):
            repository = InMemoryPaperTradingV2Repository()
            object.__setattr__(context, "paper_repository", repository)
            return repository
        return self._build_dependency(
            _default_paper_repository_factory,
            "PaperTradingV2Repository",
            binding=binding,
            trade_date=run.trade_date,
        )

    @staticmethod
    def _validate_local_sim_snapshot_for_progress(
        *, run: SimulationDailyRun, execution: SimulationExecutionResult, orders: tuple[Any, ...],
    ) -> None:
        if len(orders) != len(execution.execution_plan.intents):
            raise DataUnavailableError(
                "LocalSim execution snapshot order count does not match execution plan intents",
                context={
                    "reason_code": "LOCALSIM_DURABLE_ORDER_PLAN_MISMATCH",
                    "run_id": run.run_id,
                    "plan_id": execution.execution_plan.plan_id,
                    "expected_order_count": len(execution.execution_plan.intents),
                    "actual_order_count": len(orders),
                },
            )

    @staticmethod
    def _validate_local_sim_execution_states(
        *, binding: SimulationReleaseBinding, run: SimulationDailyRun,
        execution: SimulationExecutionResult, states: tuple[LocalSimExecutionStateV1, ...],
    ) -> None:
        expected_intents = {intent.intent_id for intent in execution.execution_plan.intents}
        actual_intents = {state.intent_id for state in states}
        if len(states) != len(actual_intents) or actual_intents != expected_intents:
            raise DataUnavailableError(
                "LocalSim durable execution states do not match execution plan intents",
                context={
                    "reason_code": "LOCALSIM_DURABLE_STATE_PLAN_MISMATCH",
                    "run_id": run.run_id,
                    "plan_id": execution.execution_plan.plan_id,
                    "expected_intent_ids": sorted(expected_intents),
                    "actual_intent_ids": sorted(actual_intents),
                    "state_count": len(states),
                },
            )
        for state in states:
            if (
                state.run_id != run.run_id or state.binding_id != binding.binding_id
                or state.plan_id != execution.execution_plan.plan_id or state.trade_date != run.trade_date
            ):
                raise DataUnavailableError(
                    "LocalSim durable execution state identity conflicts with the active run",
                    context={
                        "reason_code": "LOCALSIM_DURABLE_STATE_IDENTITY_CONFLICT",
                        "state_id": state.state_id,
                        "state_run_id": state.run_id,
                        "run_id": run.run_id,
                        "state_binding_id": state.binding_id,
                        "binding_id": binding.binding_id,
                        "state_plan_id": state.plan_id,
                        "plan_id": execution.execution_plan.plan_id,
                        "state_trade_date": state.trade_date.isoformat(),
                        "trade_date": run.trade_date.isoformat(),
                    },
                )

    def _validate_local_sim_snapshot_for_success(
        self,
        *,
        run: SimulationDailyRun,
        execution: SimulationExecutionResult,
        orders: tuple[Any, ...],
        fills: tuple[Any, ...],
        cash_entries: tuple[Any, ...],
    ) -> None:
        if len(orders) != len(execution.execution_plan.intents):
            raise DataUnavailableError(
                "LocalSim execution snapshot order count does not match execution plan intents",
                context={
                    "run_id": run.run_id,
                    "plan_id": execution.execution_plan.plan_id,
                    "expected_order_count": len(execution.execution_plan.intents),
                    "actual_order_count": len(orders),
                },
            )
        if not fills or not cash_entries:
            raise DataUnavailableError(
                "LocalSim execution cannot succeed without durable fills and cash ledger entries",
                context={
                    "run_id": run.run_id,
                    "plan_id": execution.execution_plan.plan_id,
                    "order_count": len(orders),
                    "fill_count": len(fills),
                    "cash_ledger_count": len(cash_entries),
                },
            )

    @staticmethod
    def _local_sim_persistence_failure_stage(exc: BaseException) -> str:
        context = getattr(exc, "context", None)
        if isinstance(context, dict):
            reason_code = str(context.get("reason_code") or "")
            if reason_code == "LOCALSIM_PERSISTENCE_CASH_CONTEXT_MISSING":
                return "LOCAL_SIM_PERSISTENCE_CASH_CONTEXT_MISSING"
            if reason_code.startswith("LOCALSIM_MARK_") or reason_code.startswith("LOCALSIM_SUSPENDED_"):
                return "LOCAL_SIM_MARK_VALIDATION_FAILED"
            if reason_code.startswith("LOCALSIM_PROJECTION_"):
                return "LOCAL_SIM_PROJECTION_FAILED"
            if reason_code.startswith("LOCALSIM_ECONOMIC_"):
                return "LOCAL_SIM_ECONOMIC_COMMIT_FAILED"
        message = str(exc)
        if "no execution snapshot" in message:
            return "LOCAL_SIM_PERSISTENCE_SNAPSHOT_MISSING"
        if "requires account cash or cash ledger entries" in message:
            return "LOCAL_SIM_PERSISTENCE_CASH_CONTEXT_MISSING"
        if "order count does not match" in message:
            return "LOCAL_SIM_PERSISTENCE_ORDER_MISMATCH"
        if "without durable fills and cash ledger entries" in message:
            return "LOCAL_SIM_PERSISTENCE_EMPTY_EFFECTS"
        return "LOCAL_SIM_PERSISTENCE_FAILED"

    @staticmethod
    def _ensure_local_sim_paper_run(
        *,
        repository: Any,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> None:
        portfolio_id = str(context.portfolio_id or run.strategy_id)
        existing = repository.get_run_by_portfolio_date(portfolio_id, run.trade_date)
        if existing is not None and existing.run_id != run.run_id:
            raise RuntimeConfigInvalidError(
                "LocalSim simulation runtime cannot persist into a different Paper v2 run for the same portfolio/date",
                context={
                    "simulation_run_id": run.run_id,
                    "existing_run_id": existing.run_id,
                    "portfolio_id": portfolio_id,
                    "trade_date": run.trade_date.isoformat(),
                },
            )
        if existing is None:
            data_source = MinuteDataSource(str(context.market_data_source or MinuteDataSource.DB_HISTORICAL.value))
            repository.create_run(
                PaperRun(
                    run_id=run.run_id,
                    portfolio_id=portfolio_id,
                    trade_date=run.trade_date,
                    status=RunStatus.RUNNING,
                    data_source=data_source,
                    runtime_config={
                        "source": "simulation_runtime_local_sim",
                        "simulation_run_id": run.run_id,
                        "selection_evidence_id": run.selection_evidence_id,
                        "execution_plan_id": run.execution_plan_id,
                    },
                )
            )

    @staticmethod
    def _local_sim_position_marks(
        *,
        positions: dict[str, PositionLot],
        context: SimulationRunContext,
        execution: SimulationExecutionResult,
        snapshot_time: datetime,
    ) -> tuple[dict[str, float], dict[str, LocalSimMarketMarkV1]]:
        source = str(context.market_data_source or "").strip()
        if positions and source not in {MinuteDataSource.TDX_REALTIME.value, MinuteDataSource.DB_HISTORICAL.value}:
            raise DataUnavailableError("LocalSim market mark source is missing or unsupported", context={"reason_code": "LOCALSIM_MARK_SOURCE_INVALID", "source": source or None, "plan_id": execution.execution_plan.plan_id})
        loader = getattr(context.local_broker, "load_authoritative_position_marks", None)
        if positions and not callable(loader):
            raise DataUnavailableError(
                "LocalSim account generation requires an authoritative market-mark provider",
                context={
                    "reason_code": "LOCALSIM_MARK_PROVIDER_UNAVAILABLE",
                    "plan_id": execution.execution_plan.plan_id,
                },
            )
        raw_records = (
            loader(
                symbols=tuple(positions),
                trade_date=execution.run.trade_date,
                as_of_time=snapshot_time,
                pre_trade_tradability=context.pre_trade_tradability,
            )
            if positions
            else {}
        )
        if not isinstance(raw_records, Mapping):
            raise DataUnavailableError(
                "LocalSim authoritative market-mark provider returned an invalid payload",
                context={
                    "reason_code": "LOCALSIM_MARK_SCHEMA_INVALID",
                    "plan_id": execution.execution_plan.plan_id,
                },
            )
        missing = sorted(symbol for symbol in positions if symbol not in raw_records)
        unexpected = sorted(symbol for symbol in raw_records if symbol not in positions)
        if missing or unexpected:
            raise DataUnavailableError(
                "LocalSim authoritative market-mark identities do not match persisted positions",
                context={
                    "reason_code": "LOCALSIM_MARK_IDENTITY_CONFLICT",
                    "missing_symbols": missing,
                    "unexpected_symbols": unexpected,
                    "plan_id": execution.execution_plan.plan_id,
                },
            )
        accepted: dict[str, float] = {}
        records: dict[str, LocalSimMarketMarkV1] = {}
        for symbol in sorted(positions):
            try:
                record = LocalSimMarketMarkV1.model_validate(raw_records[symbol])
            except Exception as exc:
                raise DataUnavailableError(
                    "LocalSim authoritative market mark failed schema or hash validation",
                    context={"reason_code": "LOCALSIM_MARK_SCHEMA_INVALID", "symbol": symbol},
                ) from exc
            if record.symbol != symbol:
                raise DataUnavailableError(
                    "LocalSim authoritative market mark symbol conflicts with the persisted position",
                    context={
                        "reason_code": "LOCALSIM_MARK_IDENTITY_CONFLICT",
                        "symbol": symbol,
                        "mark_symbol": record.symbol,
                    },
                )
            if record.as_of_time.replace(tzinfo=None) > snapshot_time.replace(tzinfo=None):
                raise DataUnavailableError(
                    "LocalSim authoritative market mark is later than the account snapshot",
                    context={
                        "reason_code": "LOCALSIM_MARK_AS_OF_CONFLICT",
                        "symbol": symbol,
                        "mark_as_of_time": record.as_of_time.isoformat(),
                        "snapshot_time": snapshot_time.isoformat(),
                    },
                )
            tradability = dict(context.pre_trade_tradability.get(symbol) or {})
            suspend_payload = dict(tradability.get("suspend_status") or {})
            suspended = bool(
                tradability.get("is_suspended")
                or tradability.get("suspended")
                or tradability.get("suspend_d")
                or suspend_payload.get("is_suspended")
            )
            if suspended:
                if (
                    record.provenance != LocalSimMarketMarkProvenance.SUSPENDED_PREV_CLOSE
                    or record.as_of_time.date() >= execution.run.trade_date
                ):
                    raise DataUnavailableError(
                        "LocalSim suspended mark is not proven by the previous trading-day close",
                        context={
                            "reason_code": "LOCALSIM_SUSPENDED_PREV_CLOSE_UNPROVEN",
                            "symbol": symbol,
                            "mark_as_of_time": record.as_of_time.isoformat(),
                            "source": record.source,
                        },
                    )
            else:
                expected_provenance = (
                    LocalSimMarketMarkProvenance.REALTIME_MINUTE_CLOSE
                    if source == MinuteDataSource.TDX_REALTIME.value
                    else LocalSimMarketMarkProvenance.HISTORICAL_MINUTE_CLOSE
                )
                if (
                    record.provenance != expected_provenance
                    or record.source != source
                    or record.as_of_time.date() != execution.run.trade_date
                ):
                    raise DataUnavailableError(
                        "LocalSim market mark provenance does not match the selected execution source",
                        context={
                            "reason_code": "LOCALSIM_MARK_PROVENANCE_CONFLICT",
                            "symbol": symbol,
                            "expected_source": source,
                            "actual_source": record.source,
                            "expected_provenance": expected_provenance.value,
                            "actual_provenance": record.provenance.value,
                        },
                    )
            accepted[symbol] = float(record.price)
            records[symbol] = record
        return accepted, records

    @staticmethod
    def _cash_after_local_sim(cash_entries: tuple[Any, ...], context: SimulationRunContext) -> float:
        if cash_entries:
            return float(getattr(cash_entries[-1], "cash_after"))
        if context.cash is not None:
            return float(context.cash)
        raise DataUnavailableError(
            "LocalSim persistence requires account cash or cash ledger entries",
            context={
                "reason_code": "LOCALSIM_PERSISTENCE_CASH_CONTEXT_MISSING",
                "stage": "LOCAL_SIM_PERSISTENCE",
                "required_action": (
                    "persist LocalSim cash ledger entries or provide explicit account cash; "
                    "do not infer missing cash"
                ),
            },
        )

    @staticmethod
    def _local_sim_snapshot_time(
        *,
        fills: tuple[Any, ...],
        events: tuple[Any, ...],
        run: SimulationDailyRun,
        local_broker: Any,
        market_data_source: str | None,
    ) -> datetime:
        if fills:
            return max(getattr(fill, "trade_time") for fill in fills)
        if events:
            return max(getattr(event, "event_time") for event in events)
        broker_as_of_time = getattr(local_broker, "scheduler_as_of_time", None)
        if isinstance(broker_as_of_time, datetime):
            if broker_as_of_time.date() != run.trade_date:
                raise DataUnavailableError(
                    "LocalSim broker as-of time does not match the run trade date",
                    context={
                        "reason_code": "LOCALSIM_MARK_AS_OF_DATE_CONFLICT",
                        "run_id": run.run_id,
                        "trade_date": run.trade_date.isoformat(),
                        "as_of_time": broker_as_of_time.isoformat(),
                    },
                )
            return broker_as_of_time
        if market_data_source == MinuteDataSource.TDX_REALTIME.value:
            raise DataUnavailableError(
                "LocalSim realtime account snapshot has no authoritative as-of time",
                context={
                    "reason_code": "LOCALSIM_MARK_AS_OF_TIME_MISSING",
                    "run_id": run.run_id,
                },
            )
        return datetime.combine(run.trade_date, _POST_CLOSE_RECONCILE_TIME, tzinfo=SCHEDULER_TZ)

    @staticmethod
    def _performance_marks(context: SimulationRunContext) -> dict[str, float]:
        marks = dict(context.current_prices or {})
        if context.price_by_symbol:
            marks.update({symbol: float(price) for symbol, price in context.price_by_symbol.items()})
        return marks

    @staticmethod
    def _should_drive_existing_local_sim(
        *, binding: SimulationReleaseBinding, run: SimulationDailyRun, plan: ExecutionPlan, submit: bool,
    ) -> bool:
        return (
            submit and binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM and bool(plan.intents)
            and bool(run.run_payload_json.get("broker_called"))
            and run.status == SimulationDailyRunStatus.INTRADAY_RUNNING
        )

    def _drive_existing_local_sim(
        self, *, binding: SimulationReleaseBinding, run: SimulationDailyRun, plan: ExecutionPlan,
        runtime_release: StrategyRuntimeRelease, trade_date: date, data_source: str,
        as_of_time: datetime | None,
    ) -> SimulationSchedulerBindingResult:
        context = self._load_run_context(
            runtime_release=runtime_release, binding=binding, trade_date=trade_date, as_of_time=as_of_time
        )
        self._configure_local_sim_runtime_scope(
            binding=binding, run=run, plan=plan, context=context, restore=True, as_of_time=as_of_time
        )
        broker = context.local_broker
        assert broker is not None
        advance = getattr(broker, "advance_realtime_execution", None)
        exporter = getattr(broker, "export_execution_snapshot", None)
        if not callable(advance) or not callable(exporter):
            raise RuntimeConfigInvalidError(
                "LocalSim realtime broker cannot drive/export the durable minute loop",
                context={"reason_code": "LOCALSIM_DURABLE_ADVANCE_UNSUPPORTED", "run_id": run.run_id,
                         "binding_id": binding.binding_id, "plan_id": plan.plan_id},
            )
        try:
            handles = tuple(advance(as_of_time=scheduler_time(as_of_time)))
        except Exception as exc:
            self.orchestrator.mark_submit_failure(run=run, stage="LOCAL_SIM_INTRADAY_ADVANCE_FAILED", exc=exc)
            raise
        raw_snapshot = exporter(handles=handles)
        snapshot = LocalSimExecutionSnapshot(
            orders=tuple(raw_snapshot.get("orders") or ()),
            fills=tuple(raw_snapshot.get("fills") or ()),
            events=tuple(raw_snapshot.get("events") or ()),
            cash_entries=tuple(raw_snapshot.get("cash_entries") or ()),
            positions=dict(raw_snapshot.get("positions") or {}),
            account=raw_snapshot.get("account"),
            handle_statuses=tuple(raw_snapshot.get("handle_statuses") or ()),
        )
        broker_result = LocalSimPlanSubmitResult(
            order_intents=tuple(LocalSimExecutionBridge().build_order_intents(plan)),
            handles=handles,
            execution_snapshot=snapshot,
        )
        execution = SimulationExecutionResult(
            run=run, execution_plan=plan, broker_backend=binding.broker_backend,
            status="SUBMITTED", intent_count=len(plan.intents), broker_result=broker_result,
        )
        local_persistence = self._persist_local_sim_execution_result(
            binding=binding, run=run, execution=execution, context=context
        )
        latest_run = self.repository.get_simulation_daily_run(run.run_id)
        if local_persistence is not None and not bool(local_persistence.payload.get("terminal")):
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id, strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend, status="LOCALSIM_INTRADAY_RUNNING",
                run=latest_run, execution_plan=plan, execution_result=replace(execution, run=latest_run),
                data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                    binding=binding, trade_date=trade_date, default_data_source=data_source
                ),
            )
        terminal_execution = replace(execution, run=latest_run)
        tail_result = self._handle_tail_after_submit(
            binding=binding, run=latest_run, execution=terminal_execution, context=context
        )
        reconciliation = self._reconcile_after_submit_with_timeout(
            binding=binding, run=latest_run, context=context
        )
        self._persist_strategy_performance(
            binding=binding, run=latest_run, context=context, local_persistence=local_persistence
        )
        latest_run = self.repository.get_simulation_daily_run(run.run_id)
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=self._local_sim_terminal_capacity_residual_status(
                latest_run,
                fallback=self._result_status_after_post_submit(
                    terminal_execution.status, tail_result=tail_result, reconciliation=reconciliation
                ),
            ),
            run=latest_run,
            execution_plan=plan,
            execution_result=terminal_execution,
            reconciliation_result=reconciliation,
            data_source=context.market_data_source or self._effective_market_data_source_for_binding(
                binding=binding, trade_date=trade_date, default_data_source=data_source
            ),
        )

    @staticmethod
    def _should_submit_existing_plan(
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        submit: bool,
    ) -> bool:
        if not submit or not plan.intents:
            return False
        if run.run_payload_json.get("broker_called"):
            return (
                binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
                and run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
                and SimulationLifecycleScheduler._mini_qmt_batch_has_deferred_dependent_buy(run.run_payload_json)
                and not SimulationLifecycleScheduler._mini_qmt_batch_has_open_order_evidence(run.run_payload_json)
            )
        if (
            binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
            and run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
            and (
                SimulationLifecycleScheduler._mini_qmt_batch_has_broker_side_effect_evidence(run.run_payload_json)
                or SimulationLifecycleScheduler._mini_qmt_batch_has_duplicate_order_remark(run.run_payload_json)
            )
        ):
            return False
        statuses = {
            SimulationDailyRunStatus.CREATED,
            SimulationDailyRunStatus.PRECHECKING,
            SimulationDailyRunStatus.SIGNAL_GENERATING,
            SimulationDailyRunStatus.TARGET_GENERATING,
            SimulationDailyRunStatus.PLANNING_EXECUTION,
            SimulationDailyRunStatus.FAILED_RETRYABLE,
        }
        if binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM:
            statuses.add(SimulationDailyRunStatus.SUBMITTING)
        if run.status in statuses:
            return True
        return (
            binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
            and run.status == SimulationDailyRunStatus.SUCCEEDED
            and SimulationLifecycleScheduler._mini_qmt_batch_failed_without_broker_side_effect(run.run_payload_json)
        )

    @staticmethod
    def _should_mark_existing_no_rebalance(
        *,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        submit: bool,
    ) -> bool:
        if not submit or plan.intents or run.run_payload_json.get("broker_called"):
            return False
        if SimulationLifecycleScheduler._execution_plan_has_pre_trade_blocks(plan):
            return False
        return run.status in {
            SimulationDailyRunStatus.CREATED,
            SimulationDailyRunStatus.PRECHECKING,
            SimulationDailyRunStatus.SIGNAL_GENERATING,
            SimulationDailyRunStatus.TARGET_GENERATING,
            SimulationDailyRunStatus.PLANNING_EXECUTION,
            SimulationDailyRunStatus.FAILED_RETRYABLE,
            SimulationDailyRunStatus.SUBMITTING,
        }

    @staticmethod
    def _execution_plan_has_pre_trade_blocks(plan: ExecutionPlan) -> bool:
        for decision in plan.trading_rule_decisions:
            if decision.price_limit_rule.get("pre_trade_tradability") and not bool(
                decision.price_limit_rule["pre_trade_tradability"].get("is_tradable", True)
            ):
                return True
            if str(decision.reason_code).upper() in {
                "SUSPENDED_BY_SUSPEND_D",
                "NO_TRADABLE_REALTIME_QUOTE",
                "REALTIME_QUOTE_MISSING",
                "SUSPENDED_OR_NO_QUOTE_BLOCKED",
            }:
                return True
        return False

    @staticmethod
    def _should_reconcile_existing_miniqmt_run(
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        submit: bool,
    ) -> bool:
        return (
            submit
            and binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
            and SimulationLifecycleScheduler._mini_qmt_batch_has_broker_side_effect_evidence(run.run_payload_json)
            and run.status
            in {
                SimulationDailyRunStatus.SUBMITTING,
                SimulationDailyRunStatus.INTRADAY_RUNNING,
                SimulationDailyRunStatus.RECONCILING,
                SimulationDailyRunStatus.SUCCEEDED,
                SimulationDailyRunStatus.FAILED_RETRYABLE,
            }
            and (
                SimulationLifecycleScheduler._mini_qmt_batch_succeeded(run.run_payload_json)
                or SimulationLifecycleScheduler._mini_qmt_batch_has_terminal_capacity_residual(run.run_payload_json)
                or SimulationLifecycleScheduler._mini_qmt_batch_has_open_order_evidence(run.run_payload_json)
                or SimulationLifecycleScheduler._mini_qmt_event_loop_has_submitted_children(run.run_payload_json)
                or (
                    run.status != SimulationDailyRunStatus.FAILED_RETRYABLE
                    and SimulationLifecycleScheduler._mini_qmt_batch_has_retryable_buy_residual(run.run_payload_json)
                )
            )
        )

    @staticmethod
    def _mini_qmt_event_loop_has_submitted_children(payload: dict[str, Any]) -> bool:
        for key in ("submitted_intents", "triggered_child_order_count"):
            try:
                if int(payload.get(key) or 0) > 0:
                    return True
            except (TypeError, ValueError):
                pass
        batch = payload.get("qmt_batch_result") if isinstance(payload.get("qmt_batch_result"), dict) else {}
        for key in ("succeeded", "submitted_child_count", "triggered_child_order_count"):
            try:
                if int(batch.get(key) or 0) > 0:
                    return True
            except (TypeError, ValueError):
                pass
        runtime_evidence = batch.get("runtime_evidence") if isinstance(batch.get("runtime_evidence"), dict) else {}
        try:
            return int(runtime_evidence.get("submitted_child_count") or 0) > 0
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _mini_qmt_batch_has_open_order_evidence(payload: dict[str, Any]) -> bool:
        for container_key in ("reconcile_after_submit", "sync_after_submit", "sync_before_submit"):
            container = payload.get(container_key)
            if not isinstance(container, dict):
                continue
            open_order_evidence = container.get("open_order_evidence")
            if (
                isinstance(open_order_evidence, dict)
                and int(open_order_evidence.get("open_order_count") or 0) > 0
            ):
                return True
        return False

    @staticmethod
    def _mini_qmt_batch_has_deferred_dependent_buy(payload: dict[str, Any]) -> bool:
        summary = SimulationLifecycleScheduler._mini_qmt_batch_residual_summary(payload)
        return bool(summary.get("noncompensating_residual")) and int(summary.get("dependent_buy_count") or 0) > 0

    @staticmethod
    def _mini_qmt_batch_has_retryable_buy_residual(
        payload: dict[str, Any],
        *,
        allowed_error_codes: frozenset[str] = _MINIQMT_RETRYABLE_BUY_RESIDUAL_ERROR_CODES,
    ) -> bool:
        summary = SimulationLifecycleScheduler._mini_qmt_batch_residual_summary(
            payload,
            allowed_error_codes=allowed_error_codes,
        )
        return bool(summary.get("noncompensating_residual"))

    @staticmethod
    def _mini_qmt_batch_has_noncompensating_residual(payload: dict[str, Any]) -> bool:
        summary = SimulationLifecycleScheduler._mini_qmt_batch_residual_summary(payload)
        return bool(summary.get("noncompensating_residual"))

    @staticmethod
    def _mini_qmt_batch_has_broker_side_effect_evidence(payload: dict[str, Any]) -> bool:
        if bool(payload.get("broker_called")):
            return True
        if payload.get("miniqmt_side_effect_state") == "UNKNOWN_TIMEOUT":
            return True
        if isinstance(payload.get("miniqmt_submit_timeout"), dict):
            return True
        for container_key in ("reconcile_after_submit", "sync_after_submit", "sync_before_submit"):
            container = payload.get(container_key)
            if not isinstance(container, dict):
                continue
            side_effect_evidence = container.get("side_effect_evidence")
            if (
                isinstance(side_effect_evidence, dict)
                and int(side_effect_evidence.get("broker_side_effect_count") or 0) > 0
            ):
                return True
            open_order_evidence = container.get("open_order_evidence")
            if (
                isinstance(open_order_evidence, dict)
                and int(open_order_evidence.get("open_order_count") or 0) > 0
            ):
                return True
        return False

    @staticmethod
    def _mini_qmt_batch_has_duplicate_order_remark(payload: dict[str, Any]) -> bool:
        batch = payload.get("qmt_batch_result") if isinstance(payload.get("qmt_batch_result"), dict) else {}
        results = batch.get("results")
        if not isinstance(results, list):
            return False
        for result in results:
            if not isinstance(result, dict):
                continue
            preflight = result.get("preflight") if isinstance(result.get("preflight"), dict) else {}
            errors = preflight.get("errors")
            if not isinstance(errors, list):
                continue
            for error in errors:
                if isinstance(error, dict) and str(error.get("code") or "").upper() == "DUPLICATE_ORDER_REMARK":
                    return True
        return False

    @staticmethod
    def _should_drive_existing_miniqmt_event_loop(
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        submit: bool,
    ) -> bool:
        if not submit or binding.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
            return False
        if run.status not in {
            SimulationDailyRunStatus.SUBMITTING,
            SimulationDailyRunStatus.INTRADAY_RUNNING,
            SimulationDailyRunStatus.RECONCILING,
        }:
            return False
        payload = run.run_payload_json
        route = payload.get("miniqmt_runtime_route") if isinstance(payload.get("miniqmt_runtime_route"), dict) else {}
        if str(route.get("route") or "").upper() not in {"", "A_EVENT_LOOP"}:
            return False
        return SimulationLifecycleScheduler._mini_qmt_event_loop_has_pending_algos(payload)

    @staticmethod
    def _mini_qmt_event_loop_has_pending_algos(payload: dict[str, Any]) -> bool:
        for key in ("pending_intents", "event_loop_pending_count"):
            try:
                if int(payload.get(key) or 0) > 0:
                    return True
            except (TypeError, ValueError):
                pass
        batch = payload.get("qmt_batch_result") if isinstance(payload.get("qmt_batch_result"), dict) else {}
        for key in ("pending", "pending_child_trigger_count"):
            try:
                if int(batch.get(key) or 0) > 0:
                    return True
            except (TypeError, ValueError):
                pass
        runtime_evidence = batch.get("runtime_evidence") if isinstance(batch.get("runtime_evidence"), dict) else {}
        try:
            if int(runtime_evidence.get("pending_algo_count") or 0) > 0:
                return True
        except (TypeError, ValueError):
            pass
        return str(payload.get("qmt_batch_status") or batch.get("batch_status") or "").upper() == "SUBMITTING"

    def _drive_miniqmt_event_loop_ticks_with_timeout(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        context: SimulationRunContext,
        mode: str,
        as_of_time: datetime | None,
    ) -> Any | None:
        self._prepare_miniqmt_quote_context_for_plan(
            binding=binding,
            plan=plan,
            as_of_time=as_of_time,
            recovering_active=True,
        )
        try:
            result = self._run_callable_with_timeout(
                stage="MINIQMT_EVENT_LOOP_TICK_DRIVER",
                reason_code="MINIQMT_EVENT_LOOP_TICK_DRIVER_TIMEOUT",
                timeout_env_var=SIMULATION_MINIQMT_TICK_DRIVER_TIMEOUT_ENV,
                default_timeout_seconds=DEFAULT_MINIQMT_TICK_DRIVER_TIMEOUT_SECONDS,
                context={
                    "run_id": run.run_id,
                    "plan_id": plan.plan_id,
                    "binding_id": binding.binding_id,
                    "strategy_id": binding.strategy_id,
                    "broker_backend": binding.broker_backend.value,
                    "trade_date": run.trade_date.isoformat(),
                    "qmt_batch_id": run.run_payload_json.get("qmt_batch_id"),
                    "pending_intents": run.run_payload_json.get("pending_intents"),
                    "as_of_time": as_of_time.isoformat() if isinstance(as_of_time, datetime) else None,
                },
                func=lambda: self._drive_miniqmt_event_loop_ticks(
                    binding=binding,
                    run=run,
                    plan=plan,
                    context=context,
                    mode=mode,
                    as_of_time=as_of_time,
                ),
            )
        except RuntimeConfigInvalidError as exc:
            if self._exception_context(exc).get("reason_code") == "MINIQMT_EVENT_LOOP_TICK_DRIVER_TIMEOUT":
                self._mark_miniqmt_tick_driver_timeout(binding=binding, run=run, plan=plan, exc=exc)
                return None
            raise
        self._persist_miniqmt_tick_driver_result(binding=binding, run=run, result=result)
        return result

    def _drive_miniqmt_event_loop_ticks(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        context: SimulationRunContext,
        mode: str,
        as_of_time: datetime | None,
    ) -> Any:
        if context.managed_order_service is None:
            raise DataUnavailableError(
                "MiniQMT event_loop tick driver requires QmtManagedOrderService",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_SERVICE_MISSING",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_CONTEXT",
                    "run_id": run.run_id,
                    "plan_id": plan.plan_id,
                    "binding_id": binding.binding_id,
                },
            )
        bridge = MiniQMTExecutionBridge(
            managed_order_service=context.managed_order_service,
            b0_quote_v2_controller_factory=self._b0_quote_v2_controller_factory,
        )
        return bridge.drive_event_loop_ticks(
            plan=plan,
            binding=binding,
            mode=mode,
            price_by_symbol=context.price_by_symbol or context.current_prices,
            as_of_time=as_of_time,
        )

    def _persist_miniqmt_tick_driver_result(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        result: Any,
    ) -> SimulationDailyRun:
        payload = run.run_payload_json
        result_payload = result.to_dict() if hasattr(result, "to_dict") else dict(result)
        evidence = result_payload.get("runtime_evidence") if isinstance(result_payload.get("runtime_evidence"), dict) else {}
        submitted_child_count = int(evidence.get("submitted_child_count") or result_payload.get("submitted_child_count") or 0)
        rejected_child_count = int(evidence.get("rejected_child_count") or result_payload.get("rejected_child_count") or 0)
        pending_algo_count = int(evidence.get("pending_algo_count") or result_payload.get("pending_algo_count") or 0)
        qmt_batch_id = str(payload.get("qmt_batch_id") or "").strip()
        batch_results = result_payload.get("batch_results") if isinstance(result_payload.get("batch_results"), dict) else {}
        qmt_batch_result = dict(payload.get("qmt_batch_result") if isinstance(payload.get("qmt_batch_result"), dict) else {})
        qmt_batch_status = payload.get("qmt_batch_status")
        if qmt_batch_id and isinstance(batch_results.get(qmt_batch_id), dict):
            latest_batch = batch_results[qmt_batch_id]
            result_json = latest_batch.get("result_json") if isinstance(latest_batch.get("result_json"), dict) else {}
            metadata = latest_batch.get("metadata") if isinstance(latest_batch.get("metadata"), dict) else {}
            qmt_batch_status = latest_batch.get("batch_status") or qmt_batch_status
            qmt_batch_result.update(result_json)
            qmt_batch_result.update(
                {
                    "success": rejected_child_count == 0 and (submitted_child_count > 0 or pending_algo_count > 0),
                    "batch_id": qmt_batch_id,
                    "batch_status": qmt_batch_status,
                    "total": int(qmt_batch_result.get("total") or len(qmt_batch_result.get("results") or [])),
                    "succeeded": submitted_child_count,
                    "failed": rejected_child_count,
                    "pending": pending_algo_count,
                    "triggered_child_order_count": submitted_child_count,
                    "pending_child_trigger_count": pending_algo_count,
                    "runtime_evidence": evidence,
                    "event_loop_batch_metadata": metadata,
                }
            )
        payload_patch = {
            "broker_called": bool(payload.get("broker_called")) or submitted_child_count > 0,
            "submitted_intents": submitted_child_count,
            "failed_intents": rejected_child_count,
            "pending_intents": pending_algo_count,
            "miniqmt_event_loop_tick_driver": result_payload,
            "last_stage": SimulationDailyRunStatus.INTRADAY_RUNNING.value,
        }
        if qmt_batch_status:
            payload_patch["qmt_batch_status"] = qmt_batch_status
        if qmt_batch_result:
            payload_patch["qmt_batch_result"] = qmt_batch_result
        return self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.INTRADAY_RUNNING,
            payload_patch=payload_patch,
            payload_unset=("submit_failure",),
        )

    def _mark_miniqmt_tick_driver_timeout(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        exc: RuntimeConfigInvalidError,
    ) -> SimulationDailyRun:
        context = self._exception_context(exc)
        diagnostic = {
            "schema_version": "miniqmt_event_loop_tick_driver_timeout_v1",
            "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_TIMEOUT",
            "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_TIMEOUT",
            "reason": "miniqmt_event_loop_tick_driver_exceeded_stage_timeout",
            "run_id": run.run_id,
            "plan_id": plan.plan_id,
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "trade_date": run.trade_date.isoformat(),
            "qmt_batch_id": run.run_payload_json.get("qmt_batch_id"),
            "timeout_context": context,
        }
        return self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.INTRADAY_RUNNING,
            payload_patch={
                "last_stage": SimulationDailyRunStatus.INTRADAY_RUNNING.value,
                "miniqmt_event_loop_tick_driver_timeout": diagnostic,
                "miniqmt_event_loop_tick_driver": diagnostic,
            },
        )

    @staticmethod
    def _mini_qmt_batch_has_terminal_capacity_residual(payload: dict[str, Any]) -> bool:
        summary = SimulationLifecycleScheduler._mini_qmt_batch_residual_summary(payload)
        return (
            bool(summary.get("noncompensating_residual"))
            and int(summary.get("capacity_residual_count") or 0) > 0
            and int(summary.get("dependent_buy_count") or 0) == 0
        )

    @staticmethod
    def _miniqmt_capacity_residual_observability(
        payload: dict[str, Any],
        *,
        reason: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any] | None:
        summary = SimulationLifecycleScheduler._mini_qmt_batch_residual_summary(payload)
        capacity_residual_count = int(summary.get("capacity_residual_count") or 0)
        if (
            not bool(summary.get("noncompensating_residual"))
            or capacity_residual_count <= 0
            or int(summary.get("dependent_buy_count") or 0) > 0
        ):
            return None
        submit_gate = SimulationLifecycleScheduler._latest_miniqmt_payload_evidence(payload, "submit_result_gate")
        terminalization = payload.get("miniqmt_post_close_terminalization")
        if not isinstance(terminalization, dict):
            terminalization = {}
        gate_reason = str(submit_gate.get("reason") or "")
        terminal_reason = str(terminalization.get("reason") or "")
        is_succeeded_capacity_residual = (
            bool(submit_gate.get("terminal_capacity_residual"))
            and str(submit_gate.get("status") or "").upper() == "SUCCEEDED"
        ) or terminalization.get("audit_state") == "succeeded_with_capacity_residual"
        if not is_succeeded_capacity_residual and reason not in {
            "miniqmt_capacity_residual_skipped_and_reconciled",
            "miniqmt_post_close_capacity_residual_skipped",
        }:
            return None
        batch = payload.get("qmt_batch_result") if isinstance(payload.get("qmt_batch_result"), dict) else {}
        failed_intents_raw = payload.get("failed_intents", batch.get("failed", 0))
        try:
            failed_intents = max(int(failed_intents_raw or 0), 0)
        except (TypeError, ValueError) as exc:
            raise DataUnavailableError(
                "MiniQMT capacity residual observability found invalid failed_intents",
                context={
                    "reason_code": "MINIQMT_CAPACITY_RESIDUAL_FAILED_INTENTS_INVALID",
                    "failed_intents": failed_intents_raw,
                    "qmt_batch_id": payload.get("qmt_batch_id"),
                    "qmt_batch_status": payload.get("qmt_batch_status") or batch.get("batch_status"),
                },
            ) from exc
        alert = {
            "schema_version": "simulation_runtime_alert_v1",
            "severity": "warning",
            "reason_code": "MINIQMT_SUCCEEDED_WITH_CAPACITY_RESIDUAL",
            "message": "MiniQMT run succeeded with capacity residual; monitoring must not treat it as clean success",
            "capacity_residual_count": capacity_residual_count,
            "failed_intents": failed_intents,
            "qmt_batch_id": payload.get("qmt_batch_id"),
            "qmt_batch_status": payload.get("qmt_batch_status") or batch.get("batch_status"),
        }
        return {
            "schema_version": "miniqmt_capacity_residual_observability_v1",
            "succeeded_with_capacity_residual": True,
            "reason": reason or gate_reason or terminal_reason or "miniqmt_capacity_residual_succeeded",
            "source": source
            or (
                "post_close_terminalization"
                if terminalization.get("audit_state") == "succeeded_with_capacity_residual"
                else "submit_result_gate"
            ),
            "qmt_batch_id": payload.get("qmt_batch_id"),
            "qmt_batch_status": payload.get("qmt_batch_status") or batch.get("batch_status"),
            "failed_intents": failed_intents,
            "failed_result_count": int(summary.get("failed_result_count") or 0),
            "capacity_residual_count": capacity_residual_count,
            "dependent_buy_count": int(summary.get("dependent_buy_count") or 0),
            "unknown_residual_count": int(summary.get("unknown_residual_count") or 0),
            "error_codes": list(summary.get("error_codes") or []),
            "residual_summary": summary,
            "alert": alert,
        }

    @staticmethod
    def _miniqmt_capacity_residual_payload_patch(observability: dict[str, Any] | None) -> dict[str, Any]:
        if not observability:
            return {}
        alert = observability.get("alert") if isinstance(observability.get("alert"), dict) else None
        patch: dict[str, Any] = {
            "succeeded_with_capacity_residual": True,
            "capacity_residual_count": int(observability.get("capacity_residual_count") or 0),
            "capacity_residual_failed_intents": int(observability.get("failed_intents") or 0),
            "miniqmt_capacity_residual_observability": observability,
        }
        if alert is not None:
            patch["simulation_alerts"] = [alert]
        return patch

    @staticmethod
    def _miniqmt_capacity_residual_result_fields(run: SimulationDailyRun | None) -> dict[str, Any]:
        if run is None or run.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
            return {}
        observability = SimulationLifecycleScheduler._miniqmt_capacity_residual_observability(
            run.run_payload_json
        )
        if not observability:
            return {}
        return {
            "succeeded_with_capacity_residual": True,
            "capacity_residual_count": int(observability.get("capacity_residual_count") or 0),
            "capacity_residual_failed_intents": int(observability.get("failed_intents") or 0),
            "miniqmt_capacity_residual_observability": observability,
            "alert": observability.get("alert"),
        }

    @staticmethod
    def _mini_qmt_batch_residual_summary(
        payload: dict[str, Any],
        *,
        allowed_error_codes: frozenset[str] = _MINIQMT_RETRYABLE_BUY_RESIDUAL_ERROR_CODES,
    ) -> dict[str, Any]:
        batch = payload.get("qmt_batch_result") if isinstance(payload.get("qmt_batch_result"), dict) else {}
        status = str(payload.get("qmt_batch_status") or batch.get("batch_status") or "").upper()
        summary: dict[str, Any] = {
            "schema_version": "miniqmt_batch_residual_summary_v1",
            "qmt_batch_status": status,
            "partial": status == "PARTIAL",
            "compensation_required": bool(batch.get("compensation_required")),
            "failed_result_count": 0,
            "broker_called_failed_count": 0,
            "dependent_buy_count": 0,
            "capacity_residual_count": 0,
            "unknown_residual_count": 0,
            "error_codes": [],
            "noncompensating_residual": False,
        }
        if status != "PARTIAL" or bool(batch.get("compensation_required")):
            return summary
        results = batch.get("results")
        if not isinstance(results, list):
            return summary
        error_codes_seen: set[str] = set()
        for result in results:
            if not isinstance(result, dict) or bool(result.get("success")):
                continue
            summary["failed_result_count"] = int(summary["failed_result_count"]) + 1
            if bool(result.get("broker_called")):
                summary["broker_called_failed_count"] = int(summary["broker_called_failed_count"]) + 1
                continue
            preflight = result.get("preflight") if isinstance(result.get("preflight"), dict) else {}
            errors = preflight.get("errors")
            if not isinstance(errors, list):
                summary["unknown_residual_count"] = int(summary["unknown_residual_count"]) + 1
                continue
            error_codes = {
                str(error.get("code") or "").upper()
                for error in errors
                if isinstance(error, dict) and str(error.get("code") or "").strip()
            }
            error_codes_seen.update(error_codes)
            if not error_codes or not error_codes <= allowed_error_codes:
                summary["unknown_residual_count"] = int(summary["unknown_residual_count"]) + 1
                continue
            if error_codes <= _MINIQMT_DEPENDENT_BUY_RETRY_ERROR_CODES:
                summary["dependent_buy_count"] = int(summary["dependent_buy_count"]) + 1
            elif error_codes <= _MINIQMT_CAPACITY_RESIDUAL_RETRY_ERROR_CODES:
                summary["capacity_residual_count"] = int(summary["capacity_residual_count"]) + 1
            else:
                summary["unknown_residual_count"] = int(summary["unknown_residual_count"]) + 1
        failed_count = int(summary["failed_result_count"])
        known_residual_count = int(summary["dependent_buy_count"]) + int(summary["capacity_residual_count"])
        summary["error_codes"] = sorted(error_codes_seen)
        summary["noncompensating_residual"] = (
            failed_count > 0
            and known_residual_count == failed_count
            and int(summary["broker_called_failed_count"]) == 0
            and int(summary["unknown_residual_count"]) == 0
        )
        return summary

    @staticmethod
    def _mini_qmt_batch_succeeded(payload: dict[str, Any]) -> bool:
        batch = payload.get("qmt_batch_result") if isinstance(payload.get("qmt_batch_result"), dict) else {}
        status = str(payload.get("qmt_batch_status") or batch.get("batch_status") or "").upper()
        if status not in {"SUCCEEDED", "PREVIEW_SUCCEEDED"}:
            return False
        if batch.get("success") is False:
            return False
        try:
            failed = int(batch.get("failed", payload.get("failed_intents", 0)) or 0)
        except (TypeError, ValueError):
            return False
        try:
            total = int(batch.get("total", payload.get("submitted_intents", 0)) or 0)
        except (TypeError, ValueError):
            return False
        return total > 0 and failed == 0

    def _sync_before_submit(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> dict[str, Any] | None:
        synced = self._sync_miniqmt_snapshot(
            binding=binding,
            run=run,
            context=context,
            payload_key="sync_before_submit",
        )
        return synced[0] if synced is not None else None

    def _sync_miniqmt_snapshot(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
        payload_key: str,
    ) -> tuple[dict[str, Any], Any] | None:
        if binding.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
            return None
        sync_service = getattr(context, "qmt_sync_service", None)
        if sync_service is None:
            raise DataUnavailableError(
                "MiniQMT simulation submit requires sync-before-submit service",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "broker_account_id": binding.broker_account_id,
                    "run_id": run.run_id,
                },
            )
        summary = sync_service.sync_snapshot()
        payload = summary.to_dict() if hasattr(summary, "to_dict") else dict(summary)
        self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.RECONCILING,
            payload_patch={
                "last_stage": "RECONCILING",
                payload_key: payload,
            },
        )
        return payload, summary

    def _configure_local_sim_runtime_scope(
        self, *, binding: SimulationReleaseBinding, run: SimulationDailyRun, plan: ExecutionPlan,
        context: SimulationRunContext, restore: bool, as_of_time: datetime | None,
    ) -> tuple[LocalSimExecutionStateV1, ...]:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
            return ()
        if context.market_data_source != MinuteDataSource.TDX_REALTIME.value:
            return ()
        broker = context.local_broker
        if broker is None:
            raise DataUnavailableError(
                "LocalSim realtime execution requires an instantiated broker",
                context={"reason_code": "LOCALSIM_REALTIME_BROKER_MISSING", "run_id": run.run_id,
                         "binding_id": binding.binding_id, "plan_id": plan.plan_id},
            )
        configure = getattr(broker, "configure_execution_runtime", None)
        if not callable(configure):
            raise RuntimeConfigInvalidError(
                "LocalSim realtime broker does not support durable runtime scope",
                context={"reason_code": "LOCALSIM_DURABLE_RUNTIME_UNSUPPORTED", "run_id": run.run_id,
                         "binding_id": binding.binding_id, "plan_id": plan.plan_id},
            )
        configure(run_id=run.run_id, binding_id=binding.binding_id)
        if not restore:
            return ()
        states = tuple(self.repository.list_local_sim_execution_states(run.run_id))
        if not states:
            raise DataUnavailableError(
                "LocalSim active run has no durable per-intent execution state",
                context={"reason_code": "LOCALSIM_DURABLE_STATE_MISSING", "run_id": run.run_id,
                         "binding_id": binding.binding_id, "plan_id": plan.plan_id},
            )
        by_intent = {state.intent_id: state for state in states}
        expected_intents = {intent.intent_id for intent in plan.intents}
        if set(by_intent) != expected_intents:
            raise DataUnavailableError(
                "LocalSim durable states do not close over the execution plan intents",
                context={"reason_code": "LOCALSIM_DURABLE_STATE_PLAN_MISMATCH", "run_id": run.run_id,
                         "plan_id": plan.plan_id, "expected_intent_ids": sorted(expected_intents),
                         "actual_intent_ids": sorted(by_intent)},
            )
        paper_repository = self._paper_repository_for_local_sim(binding=binding, run=run, context=context)
        orders = {order.intent_id: order for order in paper_repository.list_orders_for_run(run.run_id)}
        if set(orders) != expected_intents:
            raise DataUnavailableError(
                "LocalSim durable orders do not close over the execution plan intents",
                context={"reason_code": "LOCALSIM_DURABLE_ORDER_PLAN_MISMATCH", "run_id": run.run_id,
                         "plan_id": plan.plan_id, "expected_intent_ids": sorted(expected_intents),
                         "actual_intent_ids": sorted(orders)},
            )
        binder = getattr(broker, "bind_execution_plan", None)
        restorer = getattr(broker, "restore_execution_state", None)
        if not callable(binder) or not callable(restorer):
            raise RuntimeConfigInvalidError(
                "LocalSim realtime broker cannot restore the durable minute loop",
                context={"reason_code": "LOCALSIM_DURABLE_RESTORE_UNSUPPORTED", "run_id": run.run_id,
                         "binding_id": binding.binding_id, "plan_id": plan.plan_id},
            )
        binder(plan=plan, as_of_time=scheduler_time(as_of_time))
        for intent_id in sorted(expected_intents):
            state = by_intent[intent_id]
            if state.plan_id != plan.plan_id or state.binding_id != binding.binding_id:
                raise DataUnavailableError(
                    "LocalSim durable state identity drifted from the active plan",
                    context={"reason_code": "LOCALSIM_DURABLE_STATE_IDENTITY_CONFLICT",
                             "state_id": state.state_id, "state_plan_id": state.plan_id,
                             "plan_id": plan.plan_id, "state_binding_id": state.binding_id,
                             "binding_id": binding.binding_id},
                )
            restorer(order=orders[intent_id], state=state)
        return states

    def _submit_execution_plan_with_timeout(
        self,
        *,
        build_result: SimulationPlanBuildResult | None,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        context: SimulationRunContext,
        mode: str,
        as_of_time: datetime | None,
        submit_callable: Callable[[], SimulationExecutionResult],
    ) -> SimulationExecutionResult:
        if binding.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
            if binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM:
                self._configure_local_sim_runtime_scope(
                    binding=binding,
                    run=run,
                    plan=plan,
                    context=context,
                    restore=False,
                    as_of_time=as_of_time,
                )
            return submit_callable()
        try:
            self._prepare_miniqmt_quote_context_for_plan(
                binding=binding,
                plan=plan,
                as_of_time=as_of_time,
                recovering_active=False,
            )
        except Exception as exc:
            self._mark_miniqmt_quote_context_prepare_failure(
                binding=binding,
                run=run,
                plan=plan,
                exc=exc,
            )
            raise
        try:
            return self._run_callable_with_timeout(
                stage="MINIQMT_EVENT_LOOP_SUBMIT",
                reason_code="MINIQMT_EVENT_LOOP_SUBMIT_TIMEOUT",
                timeout_env_var=SIMULATION_MINIQMT_SUBMIT_TIMEOUT_ENV,
                default_timeout_seconds=DEFAULT_MINIQMT_SUBMIT_TIMEOUT_SECONDS,
                context={
                    "run_id": run.run_id,
                    "plan_id": plan.plan_id,
                    "binding_id": binding.binding_id,
                    "strategy_id": binding.strategy_id,
                    "broker_backend": binding.broker_backend.value,
                    "trade_date": plan.target_trade_date.isoformat(),
                    "mode": str(mode or "SIM").strip().upper(),
                    "as_of_time": as_of_time.isoformat() if isinstance(as_of_time, datetime) else None,
                    "execution_plan_intent_count": len(plan.intents),
                    "runtime_kind": MiniQMTExecutionRuntimeKind.EVENT_LOOP.value,
                    "build_result_present": build_result is not None,
                },
                func=submit_callable,
            )
        except RuntimeConfigInvalidError as exc:
            if self._exception_context(exc).get("reason_code") == "MINIQMT_EVENT_LOOP_SUBMIT_TIMEOUT":
                self._mark_miniqmt_submit_timeout(binding=binding, run=run, plan=plan, exc=exc)
            raise

    def _mark_miniqmt_quote_context_prepare_failure(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        exc: Exception,
    ) -> SimulationDailyRun:
        current = self.repository.get_simulation_daily_run(run.run_id)
        broker_side_effect_evidence = self._run_has_broker_side_effect_evidence(current)
        loud_payload = exc.as_loud_payload() if isinstance(exc, QuoteContractError) else None
        exception_context = (
            loud_payload
            if isinstance(loud_payload, dict)
            else {
                "reason_code": self._exception_context(exc).get("reason_code"),
                "stage": self._exception_context(exc).get("stage"),
                "message": str(exc),
                "context": self._exception_context(exc),
            }
        )
        retryable = not isinstance(exc, QuoteContractError) or exc.retryable
        next_status = (
            SimulationDailyRunStatus.FAILED_RETRYABLE
            if retryable or broker_side_effect_evidence
            else SimulationDailyRunStatus.FAILED_TERMINAL
        )
        diagnostic = {
            "schema_version": "miniqmt_quote_context_prepare_failure_v1",
            "reason_code": _MINIQMT_QUOTE_CONTEXT_PREPARE_FAILURE_STAGE,
            "stage": _MINIQMT_QUOTE_CONTEXT_PREPARE_FAILURE_STAGE,
            "run_id": current.run_id,
            "plan_id": plan.plan_id,
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "trade_date": plan.target_trade_date.isoformat(),
            "exception_type": type(exc).__name__,
            "exception": exception_context,
            "retryable": retryable,
            "broker_callable_invoked": False,
            "broker_side_effect_evidence_before_attempt": broker_side_effect_evidence,
            "previous_status": current.status.value,
            "next_status": next_status.value,
        }
        return self.repository.update_simulation_daily_run(
            current.run_id,
            status=next_status,
            payload_patch={
                "last_stage": _MINIQMT_QUOTE_CONTEXT_PREPARE_FAILURE_STAGE,
                "miniqmt_quote_context_prepare_failure": diagnostic,
                "submit_failure": {
                    "stage": _MINIQMT_QUOTE_CONTEXT_PREPARE_FAILURE_STAGE,
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "context": diagnostic,
                },
            },
        )

    def _recover_legacy_b0_context_missing_run_if_safe(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        submit: bool,
    ) -> SimulationDailyRun:
        if (
            not submit
            or binding.broker_backend != SimulationBrokerBackend.MINIQMT_SIM
            or run.status != SimulationDailyRunStatus.RECONCILING
        ):
            return run
        payload = run.run_payload_json if isinstance(run.run_payload_json, dict) else {}
        submit_failure = payload.get("submit_failure") if isinstance(payload.get("submit_failure"), dict) else {}
        if (
            submit_failure.get("stage") != _LEGACY_B0_CONTEXT_MISSING_FAILURE_STAGE
            or submit_failure.get("message") != _LEGACY_B0_CONTEXT_MISSING_MESSAGE
        ):
            return run
        if self._run_has_broker_side_effect_evidence(run) or self._mini_qmt_run_has_runtime_execution_evidence(payload):
            return run
        diagnostic = {
            "schema_version": "miniqmt_legacy_b0_context_missing_recovery_v1",
            "reason_code": "MINIQMT_LEGACY_B0_CONTEXT_MISSING_RECOVERED",
            "run_id": run.run_id,
            "plan_id": plan.plan_id,
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "trade_date": run.trade_date.isoformat(),
            "previous_status": run.status.value,
            "next_status": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
            "broker_called": False,
            "submitted_intents": 0,
            "broker_side_effect_evidence": False,
            "runtime_execution_evidence": False,
            "matched_failure": dict(submit_failure),
            "automatic_recovery_scope": "exact_pre_context_legacy_failure_only",
        }
        return self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.FAILED_RETRYABLE,
            payload_patch={
                "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
                "miniqmt_legacy_b0_context_missing_recovery": diagnostic,
                "submit_failure": {
                    "stage": "MINIQMT_LEGACY_B0_CONTEXT_MISSING_RECOVERY",
                    "type": "AutomaticNoSideEffectRecovery",
                    "message": "exact legacy pre-context MiniQMT failure recovered for standard retry",
                    "context": diagnostic,
                },
            },
        )

    @staticmethod
    def _mini_qmt_run_has_runtime_execution_evidence(payload: dict[str, Any]) -> bool:
        for key in (
            "qmt_batch_id",
            "qmt_batch_result",
            "miniqmt_runtime_id",
            "miniqmt_runtime_route",
            "miniqmt_event_loop_tick_driver",
            "miniqmt_runtime_evidence",
        ):
            if payload.get(key):
                return True
        return False

    def _reconcile_after_submit_with_timeout(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> dict[str, Any] | None:
        if binding.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
            return self._reconcile_after_submit(binding=binding, run=run, context=context)
        try:
            return self._run_callable_with_timeout(
                stage="MINIQMT_RECONCILE_AFTER_SUBMIT",
                reason_code="MINIQMT_RECONCILE_AFTER_SUBMIT_TIMEOUT",
                timeout_env_var=SIMULATION_MINIQMT_RECONCILE_TIMEOUT_ENV,
                default_timeout_seconds=DEFAULT_MINIQMT_RECONCILE_TIMEOUT_SECONDS,
                context={
                    "run_id": run.run_id,
                    "plan_id": run.execution_plan_id,
                    "binding_id": binding.binding_id,
                    "strategy_id": binding.strategy_id,
                    "broker_backend": binding.broker_backend.value,
                    "trade_date": run.trade_date.isoformat(),
                    "broker_called": bool(run.run_payload_json.get("broker_called")),
                    "submitted_intents": run.run_payload_json.get("submitted_intents"),
                    "qmt_batch_id": run.run_payload_json.get("qmt_batch_id"),
                },
                func=lambda: self._reconcile_after_submit(binding=binding, run=run, context=context),
            )
        except RuntimeConfigInvalidError as exc:
            if self._exception_context(exc).get("reason_code") == "MINIQMT_RECONCILE_AFTER_SUBMIT_TIMEOUT":
                self._mark_miniqmt_reconcile_timeout(binding=binding, run=run, exc=exc)
            raise

    def _mark_miniqmt_submit_timeout(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        exc: RuntimeConfigInvalidError,
    ) -> SimulationDailyRun:
        context = self._exception_context(exc)
        diagnostic = {
            "schema_version": "miniqmt_submit_timeout_v1",
            "stage": "MINIQMT_EVENT_LOOP_SUBMIT_TIMEOUT",
            "reason_code": "MINIQMT_EVENT_LOOP_SUBMIT_TIMEOUT",
            "reason": "miniqmt_event_loop_submit_exceeded_binding_stage_timeout",
            "run_id": run.run_id,
            "plan_id": plan.plan_id,
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "trade_date": plan.target_trade_date.isoformat(),
            "execution_plan_intent_count": len(plan.intents),
            "side_effect_state": "UNKNOWN_TIMEOUT",
            "runtime_kind": MiniQMTExecutionRuntimeKind.EVENT_LOOP.value,
            "error_type": type(exc).__name__,
            "message": str(exc),
            "context": context,
            "next_action": (
                "treat broker side effects as ambiguous, do not silently retry the same plan, and inspect broker "
                "orders/qmt ledger before any operator retry"
            ),
        }
        return self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.FAILED_RETRYABLE,
            payload_patch={
                "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
                "failed_intents": len(plan.intents),
                "miniqmt_side_effect_state": "UNKNOWN_TIMEOUT",
                "miniqmt_submit_timeout": diagnostic,
                "submit_failure": {
                    "stage": "MINIQMT_EVENT_LOOP_SUBMIT_TIMEOUT",
                    "outer_stage": "MINIQMT_EVENT_LOOP_SUBMIT",
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "context": diagnostic,
                },
            },
        )

    def _mark_miniqmt_reconcile_timeout(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        exc: RuntimeConfigInvalidError,
    ) -> SimulationDailyRun:
        context = self._exception_context(exc)
        diagnostic = {
            "schema_version": "miniqmt_reconcile_timeout_v1",
            "stage": "MINIQMT_RECONCILE_AFTER_SUBMIT_TIMEOUT",
            "reason_code": "MINIQMT_RECONCILE_AFTER_SUBMIT_TIMEOUT",
            "reason": "miniqmt_reconcile_after_submit_exceeded_binding_stage_timeout",
            "run_id": run.run_id,
            "plan_id": run.execution_plan_id,
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "trade_date": run.trade_date.isoformat(),
            "broker_called": bool(run.run_payload_json.get("broker_called")),
            "submitted_intents": run.run_payload_json.get("submitted_intents"),
            "qmt_batch_id": run.run_payload_json.get("qmt_batch_id"),
            "error_type": type(exc).__name__,
            "message": str(exc),
            "context": context,
            "next_action": "retry reconciliation on a later tick; do not block the scheduler thread",
        }
        return self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.FAILED_RETRYABLE,
            payload_patch={
                "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
                "reconcile_after_submit": diagnostic,
                "miniqmt_reconcile_timeout": diagnostic,
            },
        )

    def _reconcile_after_submit(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> dict[str, Any] | None:
        if binding.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
            return None
        service = getattr(context, "qmt_reconciliation_service", None)
        if service is None:
            raise DataUnavailableError(
                "MiniQMT simulation submit requires reconcile-after-submit service",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "broker_account_id": binding.broker_account_id,
                    "run_id": run.run_id,
                },
            )
        broker_positions = context.broker_positions or []
        # MiniQMT fills can appear after submit returns; sync the broker snapshot
        # again before comparing broker positions with strategy lots.
        sync_after_submit = self._sync_miniqmt_snapshot(
            binding=binding,
            run=run,
            context=context,
            payload_key="sync_after_submit",
        )
        if not broker_positions and context.managed_order_service is not None:
            broker = getattr(context.managed_order_service, "_broker", None)
            get_positions = getattr(broker, "get_positions", None)
            if callable(get_positions):
                broker_positions = list(get_positions())
        report = service.reconcile_snapshot(
            account_id=binding.broker_account_id or "",
            trade_date=run.trade_date,
            broker_positions=broker_positions,
            sync_summary=sync_after_submit[1] if sync_after_submit is not None else None,
            broker_authoritative=True,
        )
        payload = report.to_dict() if hasattr(report, "to_dict") else dict(report)
        strategy_scope = self._miniqmt_reconciliation_strategy_scope(
            report=report,
            payload=payload,
            binding=binding,
        )
        run_status_gate = self._miniqmt_reconciliation_run_status_gate(
            payload=payload,
            strategy_scope=strategy_scope,
            context=context,
        )
        batch_residual_summary = self._mini_qmt_batch_residual_summary(run.run_payload_json)
        open_order_evidence = self._miniqmt_open_order_evidence(
            binding=binding,
            run=run,
            context=context,
        )
        side_effect_evidence = self._miniqmt_side_effect_evidence(
            binding=binding,
            run=run,
            context=context,
        )
        submit_result_gate = self._miniqmt_submit_result_gate(
            run=run,
            run_status_gate=run_status_gate,
            batch_residual_summary=batch_residual_summary,
            open_order_evidence=open_order_evidence,
            side_effect_evidence=side_effect_evidence,
        )
        capacity_residual_observability = self._miniqmt_capacity_residual_observability(
            run.run_payload_json,
            reason=submit_result_gate.get("reason"),
            source="submit_result_gate",
        )
        payload = {
            **payload,
            "strategy_scope": strategy_scope,
            "run_status_gate": run_status_gate,
            "submit_result_gate": submit_result_gate,
            "qmt_batch_residual_summary": batch_residual_summary,
            "open_order_evidence": open_order_evidence,
            "side_effect_evidence": side_effect_evidence,
        }
        if capacity_residual_observability:
            payload["miniqmt_capacity_residual_observability"] = capacity_residual_observability
        if submit_result_gate["status"] == "SUCCEEDED":
            next_status = SimulationDailyRunStatus.SUCCEEDED
        elif submit_result_gate["status"] == "PENDING":
            next_status = SimulationDailyRunStatus.INTRADAY_RUNNING
        else:
            next_status = SimulationDailyRunStatus.FAILED_RETRYABLE
        payload_patch = {
            "last_stage": next_status.value,
            "reconcile_after_submit": payload,
        }
        payload_patch.update(self._miniqmt_capacity_residual_payload_patch(capacity_residual_observability))
        self.repository.update_simulation_daily_run(
            run.run_id,
            status=next_status,
            payload_patch=payload_patch,
            payload_unset=("submit_failure",) if next_status == SimulationDailyRunStatus.SUCCEEDED else None,
        )
        return payload

    @staticmethod
    def _miniqmt_submit_result_gate(
        *,
        run: SimulationDailyRun,
        run_status_gate: dict[str, Any],
        batch_residual_summary: dict[str, Any],
        open_order_evidence: dict[str, Any],
        side_effect_evidence: dict[str, Any],
    ) -> dict[str, Any]:
        batch_succeeded = SimulationLifecycleScheduler._mini_qmt_batch_succeeded(run.run_payload_json)
        pending_event_loop = SimulationLifecycleScheduler._miniqmt_pending_event_loop_evidence(
            run.run_payload_json
        )
        terminal_capacity_residual = (
            bool(batch_residual_summary.get("noncompensating_residual"))
            and int(batch_residual_summary.get("capacity_residual_count") or 0) > 0
            and int(batch_residual_summary.get("dependent_buy_count") or 0) == 0
        )
        open_order_count = int(open_order_evidence.get("open_order_count") or 0)
        broker_side_effect_count = int(side_effect_evidence.get("broker_side_effect_count") or 0)
        if open_order_count > 0:
            status = "PENDING"
            reason = "miniqmt_open_orders_pending_after_reconciliation"
        elif pending_event_loop["eligible"]:
            status = "PENDING"
            reason = "miniqmt_event_loop_pending_after_reconciliation_warning"
        elif run_status_gate.get("status") != "SUCCEEDED":
            status = "blocked"
            reason = "miniqmt_reconciliation_run_status_gate_not_succeeded"
        elif batch_succeeded:
            status = "SUCCEEDED"
            reason = "miniqmt_batch_succeeded_and_reconciled"
        elif terminal_capacity_residual:
            status = "SUCCEEDED"
            reason = "miniqmt_capacity_residual_skipped_and_reconciled"
        elif broker_side_effect_count > 0:
            status = "blocked"
            reason = "miniqmt_broker_side_effect_requires_explicit_reconciliation"
        else:
            status = "blocked"
            reason = "MiniQMT reconciliation cannot mark a run successful when submit batch did not succeed"
        return {
            "schema_version": "miniqmt_reconcile_submit_result_gate_v2",
            "status": status,
            "reason": reason,
            "qmt_batch_status": run.run_payload_json.get("qmt_batch_status"),
            "broker_called": bool(run.run_payload_json.get("broker_called")),
            "batch_succeeded": batch_succeeded,
            "terminal_capacity_residual": terminal_capacity_residual,
            "succeeded_with_capacity_residual": status == "SUCCEEDED" and terminal_capacity_residual,
            "capacity_residual_count": int(batch_residual_summary.get("capacity_residual_count") or 0),
            "failed_intents": int(run.run_payload_json.get("failed_intents") or 0),
            "open_order_count": open_order_count,
            "pending_open_orders": open_order_count > 0,
            "broker_side_effect_count": broker_side_effect_count,
            "pending_event_loop": pending_event_loop,
        }

    @staticmethod
    def _miniqmt_pending_event_loop_evidence(payload: dict[str, Any]) -> dict[str, Any]:
        batch = payload.get("qmt_batch_result") if isinstance(payload.get("qmt_batch_result"), dict) else {}
        runtime_evidence = batch.get("runtime_evidence") if isinstance(batch.get("runtime_evidence"), dict) else {}
        payload_batch_id = str(payload.get("qmt_batch_id") or "").strip()
        result_batch_id = str(batch.get("batch_id") or "").strip()
        runtime_id = str(runtime_evidence.get("runtime_id") or "").strip()
        source = str(runtime_evidence.get("source") or "").strip()
        payload_batch_status = str(payload.get("qmt_batch_status") or "").strip().upper()
        result_batch_status = str(batch.get("batch_status") or "").strip().upper()
        accepted_sources = {
            "simulation_runtime_event_loop_submit",
            "simulation_runtime_event_loop_tick_driver",
        }
        conflicts: list[str] = []

        def required_count(raw: Any, field: str) -> int | None:
            if raw is None:
                conflicts.append(f"{field}_missing")
                return None
            if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
                conflicts.append(f"{field}_invalid")
                return None
            return raw

        if not payload_batch_id:
            conflicts.append("payload_batch_id_missing")
        if not result_batch_id:
            conflicts.append("result_batch_id_missing")
        if payload_batch_id and result_batch_id and payload_batch_id != result_batch_id:
            conflicts.append("batch_id_conflict")
        if payload_batch_status != OrderBatchStatus.SUBMITTING.value:
            conflicts.append("payload_batch_not_submitting")
        if result_batch_status != OrderBatchStatus.SUBMITTING.value:
            conflicts.append("result_batch_not_submitting")
        if payload_batch_status and result_batch_status and payload_batch_status != result_batch_status:
            conflicts.append("batch_status_conflict")
        if not runtime_id:
            conflicts.append("runtime_id_missing")
        if source not in accepted_sources:
            conflicts.append("runtime_evidence_source_invalid")

        pending_counts = {
            "payload_pending_intents": required_count(payload.get("pending_intents"), "payload_pending_intents"),
            "result_pending": required_count(batch.get("pending"), "result_pending"),
            "result_pending_child_trigger_count": required_count(
                batch.get("pending_child_trigger_count"),
                "result_pending_child_trigger_count",
            ),
            "runtime_pending_algo_count": required_count(
                runtime_evidence.get("pending_algo_count"),
                "runtime_pending_algo_count",
            ),
        }
        valid_pending_counts = [value for value in pending_counts.values() if value is not None]
        pending_count = pending_counts["runtime_pending_algo_count"]
        if len(valid_pending_counts) == len(pending_counts) and len(set(valid_pending_counts)) != 1:
            conflicts.append("pending_count_conflict")
        if pending_count == 0:
            conflicts.append("no_pending_algos")

        failure_counts = {
            "payload_failed_intents": required_count(payload.get("failed_intents"), "payload_failed_intents"),
            "result_failed": required_count(batch.get("failed"), "result_failed"),
            "runtime_rejected_child_count": required_count(
                runtime_evidence.get("rejected_child_count"),
                "runtime_rejected_child_count",
            ),
        }
        valid_failure_counts = [value for value in failure_counts.values() if value is not None]
        failed_count = max(valid_failure_counts) if valid_failure_counts else None
        if any(value > 0 for value in valid_failure_counts):
            conflicts.append("failed_or_rejected_algos_present")

        active_count = required_count(runtime_evidence.get("active_algo_count"), "runtime_active_algo_count")
        if active_count is not None and pending_count is not None and active_count < pending_count:
            conflicts.append("active_algo_count_below_pending")
        return {
            "schema_version": "miniqmt_pending_event_loop_evidence_v1",
            "eligible": not conflicts,
            "batch_id": payload_batch_id or result_batch_id or None,
            "runtime_id": runtime_id or None,
            "runtime_evidence_source": source or None,
            "batch_status": payload_batch_status or result_batch_status or None,
            "active_algo_count": active_count,
            "pending_algo_count": pending_count,
            "failed_or_rejected_count": failed_count,
            "identity_sources": {
                "payload_batch_id": payload_batch_id or None,
                "result_batch_id": result_batch_id or None,
                "payload_batch_status": payload_batch_status or None,
                "result_batch_status": result_batch_status or None,
            },
            "pending_count_sources": pending_counts,
            "failure_count_sources": failure_counts,
            "conflicts": conflicts,
        }

    @staticmethod
    def _miniqmt_open_order_evidence(
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> dict[str, Any]:
        repository = getattr(context, "qmt_ledger_repository", None)
        list_order_ledger = getattr(repository, "list_order_ledger", None)
        orders = []
        if callable(list_order_ledger):
            orders = list_order_ledger(
                account_id=binding.broker_account_id,
                trade_date=run.trade_date,
                strategy_id=binding.strategy_id,
                batch_id=run.run_payload_json.get("qmt_batch_id"),
            )
        open_orders = [
            order
            for order in orders
            if is_open_like_order_status(getattr(order, "order_status", None))
            and int(getattr(order, "order_volume", 0) or 0) > int(getattr(order, "traded_volume", 0) or 0)
        ]
        return {
            "schema_version": "miniqmt_open_order_evidence_v1",
            "source": "qmt_strategy.order_ledger",
            "account_id": binding.broker_account_id,
            "strategy_id": binding.strategy_id,
            "trade_date": run.trade_date.isoformat(),
            "qmt_batch_id": run.run_payload_json.get("qmt_batch_id"),
            "open_order_count": len(open_orders),
            "open_orders": [
                {
                    "qmt_order_id": order.qmt_order_id,
                    "symbol": order.symbol,
                    "order_type": order.order_type,
                    "order_volume": order.order_volume,
                    "traded_volume": order.traded_volume,
                    "order_status": order.order_status,
                    "status_msg": order.status_msg,
                    "order_remark": order.order_remark,
                }
                for order in open_orders
            ],
        }

    @staticmethod
    def _miniqmt_side_effect_evidence(
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> dict[str, Any]:
        repository = getattr(context, "qmt_ledger_repository", None)
        list_order_ledger = getattr(repository, "list_order_ledger", None)
        orders = []
        if callable(list_order_ledger):
            orders = list_order_ledger(
                account_id=binding.broker_account_id,
                trade_date=run.trade_date,
                strategy_id=binding.strategy_id,
                batch_id=run.run_payload_json.get("qmt_batch_id"),
            )
        order_count = len(orders)
        return {
            "schema_version": "miniqmt_broker_side_effect_evidence_v1",
            "source": "qmt_strategy.order_ledger",
            "account_id": binding.broker_account_id,
            "strategy_id": binding.strategy_id,
            "trade_date": run.trade_date.isoformat(),
            "qmt_batch_id": run.run_payload_json.get("qmt_batch_id"),
            "broker_called": bool(run.run_payload_json.get("broker_called")),
            "order_ledger_count": order_count,
            "broker_side_effect_count": order_count,
        }

    @staticmethod
    def _miniqmt_reconciliation_strategy_scope(
        *,
        report: Any,
        payload: dict[str, Any],
        binding: SimulationReleaseBinding,
    ) -> dict[str, Any]:
        strategy_scope = getattr(report, "strategy_scope", None)
        if callable(strategy_scope):
            return strategy_scope(strategy_id=binding.strategy_id, strategy_name=binding.strategy_name)
        quantities = payload.get("strategy_lot_quantities") if isinstance(payload.get("strategy_lot_quantities"), dict) else {}
        strategy_name = str(binding.strategy_name or "").strip()
        matched = bool(strategy_name and strategy_name in quantities)
        scoped_quantities = quantities.get(strategy_name, {}) if matched else {}
        return {
            "schema_version": "miniqmt_reconciliation_strategy_scope_v1",
            "strategy_id": binding.strategy_id,
            "strategy_name": binding.strategy_name,
            "matched": matched,
            "status": str(payload.get("run", {}).get("status") or "WARNING") if matched else "WARNING",
            "issue_count": int(payload.get("run", {}).get("summary_json", {}).get("issue_count") or 0),
            "issue_types": [],
            "issue_symbols": sorted(scoped_quantities) if isinstance(scoped_quantities, dict) else [],
            "account_level_issue_count": 0,
            "position_count": len(scoped_quantities) if isinstance(scoped_quantities, dict) else 0,
            "symbols": sorted(scoped_quantities) if isinstance(scoped_quantities, dict) else [],
            "strategy_lot_quantities": scoped_quantities if isinstance(scoped_quantities, dict) else {},
        }

    @staticmethod
    def _miniqmt_reconciliation_run_status_gate(
        *,
        payload: dict[str, Any],
        strategy_scope: dict[str, Any],
        context: SimulationRunContext,
    ) -> dict[str, Any]:
        current_issue_count = int(strategy_scope.get("issue_count") or 0)
        diagnostics = context.context_diagnostics.get("miniqmt_broker_position_reconciliation")
        diagnostic_adjustment = _miniqmt_reconciliation_diagnostic_adjustment_symbols(diagnostics)
        issue_symbols = {
            str(symbol)
            for symbol in strategy_scope.get("issue_symbols", [])
            if str(symbol).strip()
        }
        matched = bool(strategy_scope.get("matched"))
        if not matched:
            status = "WARNING"
            reason = "strategy_scope_not_matched"
        elif current_issue_count <= 0:
            status = "SUCCEEDED"
            reason = "strategy_scope_has_no_blocking_issues"
        elif issue_symbols and issue_symbols <= diagnostic_adjustment:
            status = "SUCCEEDED"
            reason = "strategy_scope_issues_already_reconciled_by_context_provider"
        else:
            status = "WARNING"
            reason = "strategy_scope_has_blocking_issues"
        return {
            "schema_version": "miniqmt_reconciliation_run_status_gate_v1",
            "status": status,
            "reason": reason,
            "strategy_scope_issue_count": current_issue_count,
            "account_level_issue_count": int(strategy_scope.get("account_level_issue_count") or 0),
            "context_reconciled_symbol_count": len(diagnostic_adjustment),
            "strategy_scope_matched": matched,
        }

    def _validate_fresh_selection_evidence(
        self,
        *,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        selection: StrategyPackageSelectionResult,
        trade_date: date,
    ) -> None:
        evidence = selection.evidence_by_package.get(binding.package_id)
        if evidence is None:
            raise DataUnavailableError(
                "simulation scheduler requires fresh daily selection evidence for the binding package",
                context={
                    "package_id": binding.package_id,
                    "binding_id": binding.binding_id,
                    "trade_date": trade_date.isoformat(),
                },
            )
        self._validate_fresh_daily_selection_evidence(
            binding=binding,
            runtime_release=runtime_release,
            evidence=evidence,
            trade_date=trade_date,
            runtime_config=selection.runtime_config,
        )

    def _validate_fresh_daily_selection_evidence(
        self,
        *,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        evidence: DailySelectionEvidence,
        trade_date: date,
        runtime_config: dict[str, Any] | None,
    ) -> None:
        stale_reasons: list[str] = []
        if evidence.target_trade_date != trade_date:
            stale_reasons.append("target_trade_date")
        if evidence.package_id != binding.package_id:
            stale_reasons.append("package_id")
        if evidence.manifest_sha256 != binding.manifest_sha256 or evidence.manifest_sha256 != runtime_release.manifest_sha256:
            stale_reasons.append("manifest_sha256")
        if evidence.release_id != runtime_release.release_id or evidence.release_hash != runtime_release.release_hash:
            stale_reasons.append("runtime_release")
        expected_cutoff = self._expected_rolling_pit_cutoff_date(
            runtime_config=runtime_config,
            trade_date=trade_date,
        )
        if expected_cutoff is not None and evidence.cutoff_date != expected_cutoff:
            stale_reasons.append("cutoff_date")
        if stale_reasons:
            raise DataUnavailableError(
                "simulation scheduler rejected stale daily selection evidence",
                context={
                    "reasons": stale_reasons,
                    "evidence_id": evidence.evidence_id,
                    "target_trade_date": evidence.target_trade_date.isoformat(),
                    "expected_trade_date": trade_date.isoformat(),
                    "cutoff_date": evidence.cutoff_date.isoformat() if evidence.cutoff_date else None,
                    "expected_cutoff_date": expected_cutoff.isoformat() if expected_cutoff else None,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                },
            )

    def _expected_rolling_pit_cutoff_date(
        self,
        *,
        runtime_config: dict[str, Any] | None,
        trade_date: date,
    ) -> date | None:
        config = runtime_config if isinstance(runtime_config, dict) else {}
        artifact_config = StrategyPackageSelectionService.selection_artifact_config(config)
        pit_mode = StrategyPackageSelectionService.selection_pit_mode(config, artifact_config=artifact_config)
        if pit_mode == "NONE" or StrategyPackageSelectionService.is_fixed_cutoff_replay_config(
            config,
            artifact_config=artifact_config,
        ):
            return None
        resolver = getattr(self.selection_service, "resolve_point_in_time_context", None)
        if not callable(resolver):
            return None
        context = resolver(trade_date=trade_date, pit_mode=pit_mode, explicit_cutoff_date=None)
        raw_cutoff = context.get("cutoff_date") if isinstance(context, dict) else None
        if raw_cutoff is None:
            return None
        try:
            return date.fromisoformat(str(raw_cutoff))
        except ValueError as exc:
            raise RuntimeConfigInvalidError(
                "point-in-time selection cutoff_date must be YYYY-MM-DD",
                context={"cutoff_date": raw_cutoff, "trade_date": trade_date.isoformat()},
            ) from exc

    def _build_plan_from_selection(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        selection: StrategyPackageSelectionResult,
        context: SimulationRunContext,
        created_by: str,
        require_realtime_quote: bool | None = None,
        as_of_time: datetime | None = None,
        preserved_causality_cursor: datetime | None = None,
    ) -> SimulationPlanBuildResult:
        evidence = selection.evidence_by_package[binding.package_id]
        candidates = selection.package_results.get(binding.package_id, [])
        pre_trade_tradability = self._pre_trade_tradability_for_planning(
            binding=binding,
            trade_date=trade_date,
            context=context,
            candidate_symbols=[candidate.symbol for candidate in candidates],
            require_realtime_quote=require_realtime_quote,
            as_of_time=as_of_time,
        )
        snapshot = SignalSnapshot(
            package_id=binding.package_id,
            manifest_sha256=evidence.manifest_sha256,
            trade_date=trade_date,
            data_source=data_source,
            candidates=candidates,
            runtime_config=selection.runtime_config,
            valid_no_candidate=selection.valid_no_candidate,
            no_candidate_reason=selection.no_candidate_reason,
        )
        target_total_equity, target_equity_context = self._target_equity_basis_for_context(
            binding=binding,
            context=context,
        )
        build_result = self.orchestrator.build_execution_plan(
            runtime_release=runtime_release,
            binding=binding,
            selection_evidence=evidence,
            signal_snapshot=snapshot,
            current_positions=context.current_positions,
            current_prices=context.current_prices,
            pre_trade_tradability=pre_trade_tradability,
            manifest=context.manifest,
            portfolio_id=context.portfolio_id or binding.strategy_id,
            top_k=context.top_k,
            execution_policy_payload=context.execution_policy_payload,
            tail_policy_payload=context.tail_policy_payload,
            target_total_equity=target_total_equity,
            target_equity_context=target_equity_context,
            created_by=created_by,
        )
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
            return build_result
        return self._attach_local_sim_causality_cursor(
            build_result=build_result,
            as_of_time=as_of_time,
            preserved_cursor=preserved_causality_cursor,
        )

    @staticmethod
    def _local_sim_plan_causality_cursor(plan: ExecutionPlan | None) -> datetime | None:
        if plan is None:
            return None
        payload = plan.plan_payload_json.get("local_sim_execution_causality")
        if not isinstance(payload, dict):
            return None
        raw = payload.get("eligible_bar_after")
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(str(raw))
        except ValueError as exc:
            raise RuntimeConfigInvalidError(
                "LocalSim execution plan has an invalid causality cursor",
                context={"plan_id": plan.plan_id, "eligible_bar_after": raw},
            ) from exc
        return scheduler_time(parsed)

    def _attach_local_sim_causality_cursor(
        self,
        *,
        build_result: SimulationPlanBuildResult,
        as_of_time: datetime | None,
        preserved_cursor: datetime | None,
    ) -> SimulationPlanBuildResult:
        plan = build_result.execution_plan
        local_as_of = scheduler_time(as_of_time)
        cursor = scheduler_time(preserved_cursor) if preserved_cursor is not None else None
        cursor_source = "preserved_execution_plan"
        if cursor is None:
            windows = compute_schedule_windows(
                trade_date=plan.target_trade_date,
                as_of_time=local_as_of,
            )
            submit_windows = [window for window in windows if window.get("action") == "submit"]
            active = next((window for window in submit_windows if window.get("state") == "ACTIVE"), None)
            if active is not None:
                cursor = local_as_of
                cursor_source = "first_plan_during_submit_window"
            else:
                next_window = next(
                    (
                        window
                        for window in submit_windows
                        if datetime.fromisoformat(str(window["start_at"])) > local_as_of
                    ),
                    None,
                )
                if next_window is not None:
                    cursor = datetime.fromisoformat(str(next_window["start_at"])) - timedelta(microseconds=1)
                    cursor_source = "next_submit_window_boundary"
                else:
                    cursor = local_as_of
                    cursor_source = "after_last_submit_window"

        causality = {
            "schema_version": "local_sim_execution_causality_v1",
            "eligible_bar_after": cursor.isoformat(),
            "captured_as_of_time": local_as_of.isoformat(),
            "cursor_source": cursor_source,
            "bar_selection_rule": "strictly_after_cursor_and_not_after_scheduler_as_of",
        }
        payload = deepcopy(plan.plan_payload_json)
        payload["local_sim_execution_causality"] = causality
        payload_intents = payload.get("intents")
        if not isinstance(payload_intents, list):
            raise RuntimeConfigInvalidError(
                "LocalSim execution plan payload is missing intents",
                context={"plan_id": plan.plan_id},
            )
        updated_intents = [
            intent.model_copy(
                update={
                    "metadata": {
                        **dict(intent.metadata),
                        "local_sim_execution_causality": causality,
                    },
                }
            )
            for intent in plan.intents
        ]
        by_intent_id = {intent.intent_id: intent for intent in updated_intents}
        for item in payload_intents:
            if not isinstance(item, dict):
                raise RuntimeConfigInvalidError(
                    "LocalSim execution plan contains an invalid intent payload",
                    context={"plan_id": plan.plan_id},
                )
            updated = by_intent_id.get(str(item.get("intent_id") or ""))
            if updated is None:
                raise RuntimeConfigInvalidError(
                    "LocalSim execution plan intent payload cannot be reconstructed",
                    context={"plan_id": plan.plan_id, "intent_id": item.get("intent_id")},
                )
            item["metadata"] = dict(updated.metadata)
        new_hash = canonical_json_sha256(payload)
        new_id = f"plan_{new_hash[:16]}"
        updated_intents = [intent.model_copy(update={"plan_id": new_id}) for intent in updated_intents]
        prepared_plan = plan.model_copy(
            update={
                "plan_id": new_id,
                "intents": updated_intents,
                "plan_payload_json": payload,
                "plan_hash": new_hash,
                "created_at": datetime.now(UTC),
            }
        )
        persisted_plan = self.repository.save_execution_plan(prepared_plan)
        updated_run = self.repository.update_simulation_daily_run(
            build_result.run.run_id,
            execution_plan=persisted_plan,
            payload_patch={"local_sim_execution_causality": causality},
        )
        return replace(build_result, run=updated_run, execution_plan=persisted_plan)

    @staticmethod
    def _target_equity_basis_for_context(
        *,
        binding: SimulationReleaseBinding,
        context: SimulationRunContext,
    ) -> tuple[float | None, dict[str, Any] | None]:
        if context.target_total_equity is not None:
            return float(context.target_total_equity), dict(context.target_equity_context or {})
        cash = context.cash
        frozen_cash = context.frozen_cash
        if cash is None:
            if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM and context.qmt_ledger_repository is not None:
                account = context.qmt_ledger_repository.get_virtual_account(binding.strategy_id)
                cash = float(account.cash)
                frozen_cash = float(account.frozen_cash)
            elif binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM and (
                context.qmt_sync_service is not None
                or context.qmt_reconciliation_service is not None
                or bool(context.broker_positions)
            ):
                raise DataUnavailableError(
                    "MiniQMT target sizing requires strategy-slot dynamic cash/equity context",
                    context={"strategy_id": binding.strategy_id, "binding_id": binding.binding_id},
                )
            else:
                return None, None
        return _build_dynamic_target_equity_basis(
            binding=binding,
            cash=float(cash),
            frozen_cash=float(frozen_cash),
            positions=context.current_positions,
            prices=SimulationLifecycleScheduler._performance_marks(context),
            source=(
                "miniqmt_strategy_slot_dynamic_equity"
                if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
                else "paper_v2_portfolio_dynamic_equity"
            ),
        )

    def _pre_trade_tradability_for_planning(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        context: SimulationRunContext,
        candidate_symbols: list[str],
        require_realtime_quote: bool | None = None,
        as_of_time: datetime | None = None,
    ) -> dict[str, dict[str, Any]]:
        symbols = sorted({*context.current_positions.keys(), *[str(symbol).strip() for symbol in candidate_symbols if str(symbol).strip()]})
        loader = getattr(self.context_provider, "load_pre_trade_tradability", None)
        refresh_from_loader = require_realtime_quote is True and callable(loader)
        statuses = (
            {}
            if refresh_from_loader
            else {str(symbol): dict(status) for symbol, status in (context.pre_trade_tradability or {}).items()}
        )
        missing = list(symbols) if refresh_from_loader else [symbol for symbol in symbols if symbol not in statuses]
        if missing and callable(loader):
            loader_kwargs = {
                "symbols": missing,
                "trade_date": trade_date,
                "binding": binding,
                "market_data_source": context.market_data_source,
                "require_realtime_quote": require_realtime_quote,
                "as_of_time": as_of_time,
            }
            signature = inspect.signature(loader)
            accepts_kwargs = any(
                parameter.kind == inspect.Parameter.VAR_KEYWORD
                for parameter in signature.parameters.values()
            )
            supported_kwargs = {
                key: value
                for key, value in loader_kwargs.items()
                if accepts_kwargs or key in signature.parameters
            }
            statuses.update(loader(**supported_kwargs))
        return statuses

    def _persist_no_rebalance_evidence(
        self,
        *,
        build_result: SimulationPlanBuildResult,
        current_positions: dict[str, PositionLot],
    ) -> SimulationPlanBuildResult:
        if build_result.rebalance.order_intents:
            return build_result

        target_by_symbol = {target.symbol: target for target in build_result.target_positions}
        decision_by_symbol = {decision.symbol: decision for decision in build_result.rebalance.trading_rule_decisions}
        rows: list[dict[str, Any]] = []
        for symbol in sorted(set(target_by_symbol) | set(current_positions)):
            target = target_by_symbol.get(symbol)
            position = current_positions.get(symbol)
            current_quantity = int(position.quantity) if position is not None else 0
            target_quantity = int(target.target_quantity) if target is not None else 0
            decision = decision_by_symbol.get(symbol)
            rows.append(
                {
                    "symbol": symbol,
                    "target_quantity": target_quantity,
                    "current_quantity": current_quantity,
                    "delta_quantity": target_quantity - current_quantity,
                    "current_available_quantity": int(position.available_quantity) if position is not None else None,
                    "current_position_trade_date": position.trade_date.isoformat() if position is not None else None,
                    "target_weight": float(target.target_weight) if target is not None and target.target_weight is not None else None,
                    "reference_price": float(target.reference_price) if target is not None and target.reference_price is not None else None,
                    "target_reason": target.reason if target is not None else "DROPPED_FROM_SELECTION",
                    "trading_rule_decision": {
                        "decision_id": decision.decision_id,
                        "decision": decision.decision,
                        "reason_code": decision.reason_code,
                        "requested_quantity": int(decision.requested_quantity),
                        "legal_quantity": int(decision.legal_quantity),
                    }
                    if decision is not None
                    else None,
                }
            )

        reason_code = (
            "TOP_LIST_AND_QUANTITY_MATCH"
            if rows and all(row["delta_quantity"] == 0 for row in rows)
            else "NO_EMITTABLE_REBALANCE_INTENTS"
        )
        evidence_payload = {
            "schema_version": "no_rebalance_evidence_v1",
            "source": "simulation_lifecycle_scheduler",
            "reason_code": reason_code,
            "target_trade_date": build_result.selection_evidence.target_trade_date.isoformat(),
            "package_id": build_result.runtime_release.package_id,
            "manifest_sha256": build_result.runtime_release.manifest_sha256,
            "release_id": build_result.runtime_release.release_id,
            "binding_id": build_result.binding.binding_id,
            "strategy_id": build_result.binding.strategy_id,
            "broker_backend": build_result.binding.broker_backend.value,
            "selection_evidence_id": build_result.selection_evidence.evidence_id,
            "execution_plan_id": build_result.execution_plan.plan_id,
            "selected_symbols": [
                candidate.symbol
                for candidate in sorted(build_result.signal_snapshot.candidates, key=lambda item: item.rank)
            ],
            "target_symbols": sorted(target_by_symbol),
            "current_symbols": sorted(current_positions),
            "target_count": len(build_result.target_positions),
            "current_position_count": len(current_positions),
            "trading_rule_decision_count": len(build_result.rebalance.trading_rule_decisions),
            "rows": rows,
        }
        updated_run = self.repository.update_simulation_daily_run(
            build_result.run.run_id,
            payload_patch={"no_rebalance_evidence": evidence_payload},
        )
        return replace(build_result, run=updated_run)

    def _handle_tail_after_submit(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        execution: SimulationExecutionResult,
        context: SimulationRunContext,
    ) -> dict[str, Any] | None:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
            return None
        service = context.tail_policy_service
        if service is None:
            return None
        broker_result = execution.broker_result
        handles = getattr(broker_result, "handles", None)
        if not handles:
            return None
        if context.local_broker is None:
            raise DataUnavailableError(
                "LocalSim tail handling requires the original broker instance",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "run_id": run.run_id,
                    "plan_id": execution.execution_plan.plan_id,
                },
            )
        result = service.handle_local_sim_tail(
            plan=execution.execution_plan,
            broker=context.local_broker,
            handles=handles,
        )
        payload = result.to_dict()
        next_status = SimulationDailyRunStatus.SUCCEEDED if result.success else SimulationDailyRunStatus.FAILED_RETRYABLE
        self.repository.update_simulation_daily_run(
            run.run_id,
            status=next_status,
            payload_patch={
                "tail_handling": payload,
                "last_stage": next_status.value,
            },
            payload_unset=("submit_failure",) if next_status == SimulationDailyRunStatus.SUCCEEDED else None,
        )
        return payload

    @staticmethod
    def _result_status_after_post_submit(
        execution_status: str,
        *,
        tail_result: dict[str, Any] | None,
        reconciliation: dict[str, Any] | None,
    ) -> str:
        if tail_result is not None:
            return "TAIL_HANDLED" if tail_result.get("success") else "TAIL_HANDLING_FAILED"
        if reconciliation is not None:
            run = reconciliation.get("run") if isinstance(reconciliation.get("run"), dict) else {}
            submit_result_gate = (
                reconciliation.get("submit_result_gate")
                if isinstance(reconciliation.get("submit_result_gate"), dict)
                else {}
            )
            run_status_gate = (
                reconciliation.get("run_status_gate")
                if isinstance(reconciliation.get("run_status_gate"), dict)
                else {}
            )
            status = submit_result_gate.get("status") or run_status_gate.get("status") or run.get("status")
            if status == "PENDING":
                return "RECONCILIATION_PENDING_OPEN_ORDERS"
            if execution_status not in {"SUBMITTED", "RECOVERED"}:
                return "BROKER_SUBMIT_FAILED_RECONCILED" if status == "SUCCEEDED" else "BROKER_SUBMIT_FAILED"
            return "RECONCILED" if status == "SUCCEEDED" else "RECONCILIATION_WARNING"
        return execution_status

    @staticmethod
    def _local_sim_terminal_capacity_residual_status(run: SimulationDailyRun, *, fallback: str) -> str:
        if run.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
            return fallback
        if run.status != SimulationDailyRunStatus.FAILED_TERMINAL:
            return fallback
        terminalization = run.run_payload_json.get("local_sim_capacity_residual_terminalization")
        if isinstance(terminalization, dict):
            return "LOCALSIM_CAPACITY_RESIDUAL_TERMINAL"
        return fallback

    @staticmethod
    def _compute_schedule_windows(*, trade_date: date, as_of_time: datetime | None) -> tuple[dict[str, Any], ...]:
        return compute_schedule_windows(trade_date=trade_date, as_of_time=as_of_time)

    @staticmethod
    def _scheduler_now() -> datetime:
        return scheduler_now()

    @staticmethod
    def _scheduler_time(value: datetime | None) -> datetime:
        return scheduler_time(value)


simulation_lifecycle_scheduler = build_simulation_lifecycle_scheduler_from_env()


class SimulationLifecycleBackgroundScheduler:
    """Opt-in unattended scheduler wrapper with trading-window orchestration."""

    def __init__(
        self,
        *,
        lifecycle_scheduler: SimulationLifecycleScheduler | None = None,
        trading_calendar_service: Any | None = None,
        tca_eod_observation_hook: TcaEodObservationHook | None = None,
        tca_observation_metrics_emitter: TcaObservationMetricsEmitter | None = None,
    ) -> None:
        self.lifecycle_scheduler = lifecycle_scheduler or SimulationLifecycleScheduler()
        self._trading_calendar_service = trading_calendar_service or TradingCalendarStatusService()
        self._tca_eod_observation_hook = tca_eod_observation_hook or TcaEodObservationHook()
        self._tca_observation_metrics_emitter = tca_observation_metrics_emitter or TcaObservationMetricsEmitter()
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._interval_seconds = self._default_interval()
        self._default_submit = self._env_flag("SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT", default=False)
        self._data_source = (os.getenv("SIMULATION_RUNTIME_SCHEDULER_DATA_SOURCE") or "DB_HISTORICAL").strip() or "DB_HISTORICAL"
        self._limit = self._default_limit()
        self._last_run_at: datetime | None = None
        self._last_result: dict[str, Any] | None = None

    def start(self, *, interval_seconds: int | None = None, default_submit: bool | None = None) -> dict[str, Any]:
        interval = int(interval_seconds or self._interval_seconds)
        if interval <= 0:
            raise ValueError("simulation runtime scheduler interval_seconds must be positive")
        with self._lock:
            self._interval_seconds = interval
            if default_submit is not None:
                self._default_submit = bool(default_submit)
            if self._thread and self._thread.is_alive():
                return self.status()
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name="simulation-runtime-lifecycle-scheduler",
                daemon=True,
            )
            self._thread.start()
            logger.info(
                "Simulation runtime scheduler started interval=%ss default_submit=%s",
                self._interval_seconds,
                self._default_submit,
            )
            return self.status()

    def shutdown(self, wait: bool = False) -> dict[str, Any]:
        with self._lock:
            self._stop_event.set()
            thread = self._thread
        if wait and thread and thread.is_alive():
            thread.join(timeout=5.0)
        thread_alive = bool(thread and thread.is_alive())
        shutdown_selection = getattr(self.lifecycle_scheduler, "shutdown_selection_inference", None)
        if callable(shutdown_selection):
            graceful = bool(wait and not thread_alive)
            shutdown_selection(wait=graceful)
            logger.info(
                "Simulation runtime scheduler selection inference executor stopped wait=%s thread_alive=%s",
                graceful,
                thread_alive,
            )
        shutdown_quote_ingress = getattr(self.lifecycle_scheduler, "shutdown_miniqmt_quote_ingress", None)
        if callable(shutdown_quote_ingress):
            shutdown_quote_ingress()
            logger.info("Simulation runtime scheduler MiniQMT quote ingress stopped")
        logger.info("Simulation runtime scheduler stopped")
        return self.status()

    def status(self) -> dict[str, Any]:
        thread = self._thread
        base = self.lifecycle_scheduler.status()
        running = bool(thread and thread.is_alive() and not self._stop_event.is_set())
        return {
            **base,
            "autostart": running,
            "running": running,
            "thread_alive": bool(thread and thread.is_alive()),
            "scheduler_control_api_enabled": True,
            "manual_tick_endpoint_enabled": True,
            "interval_seconds": self._interval_seconds,
            "default_submit": self._default_submit,
            "data_source": self._data_source,
            "data_source_policy": self._data_source_policy(),
            "limit": self._limit,
            "last_run_at": self._last_run_at.isoformat() if self._last_run_at else None,
            "last_result": self._last_result,
            "trading_calendar_policy": self._trading_calendar_policy(),
        }

    def run_once(self, *, as_of_time: datetime | None = None) -> dict[str, Any]:
        now = SimulationLifecycleScheduler._scheduler_time(as_of_time)
        trade_date = self._trade_date(now)
        decision = self._window_decision(as_of_time=now, trade_date=trade_date)
        result: dict[str, Any] = {
            "started_at": now.isoformat(),
            "trade_date": trade_date.isoformat(),
            "timezone": SCHEDULER_TZ_NAME,
            "data_source": self._data_source,
            "data_source_policy": self._data_source_policy(),
            "window": decision["window"],
            "should_run": False,
            "submit": False,
            "reason": None,
            "trading_calendar": None,
            "processed": [],
            "errors": [],
            "alerts": [],
        }
        try:
            calendar_status = self._trading_day_status(trade_date=trade_date)
        except DataUnavailableError as exc:
            payload = {
                "type": type(exc).__name__,
                "message": str(exc),
                "context": getattr(exc, "context", None),
            }
            result["reason"] = "trading_calendar_unavailable"
            result["errors"].append(payload)
            logger.warning("Simulation runtime scheduler trading calendar gate failed: %s", payload)
            return self._record_result(started_at=now, result=result)
        result["trading_calendar"] = calendar_status
        if not bool(calendar_status.get("is_trading_day")):
            result["reason"] = "non_trading_day"
            result["skip_reason"] = "non_trading_day"
            result["next_trading_day"] = calendar_status.get("next_trading_day")
            return self._record_result(started_at=now, result=result)
        result["should_run"] = decision["should_run"]
        result["submit"] = decision["submit"]
        result["reason"] = decision["reason"]
        if decision["should_run"]:
            try:
                if decision["reason"] == "eod_reconcile":
                    tick = self.lifecycle_scheduler.post_close_reconcile_once(
                        trade_date=trade_date,
                        data_source=self._data_source,
                        limit=self._limit,
                        as_of_time=now,
                    )
                else:
                    tick = self.lifecycle_scheduler.run_once(
                        trade_date=trade_date,
                        data_source=self._data_source,
                        limit=self._limit,
                        submit=bool(decision["submit"]),
                        as_of_time=now,
                    )
                observation_outcomes: list[dict[str, Any]] = []
                if decision["reason"] == "eod_reconcile":
                    observation_outcomes = self._run_tca_eod_observation(
                        terminalized_runs=tuple(tick.stale_run_results),
                        trade_date=trade_date,
                        as_of_time=now,
                    )
                processed = []
                alerts = []
                if observation_outcomes:
                    result["tca_eod_observation"] = observation_outcomes
                    metrics, observation_alerts = self._emit_tca_observation_metrics(
                        outcomes=observation_outcomes,
                        trade_date=trade_date,
                    )
                    result["tca_eod_observation_metrics"] = metrics
                    alerts.extend(observation_alerts)
                for item in tick.results:
                    capacity_fields = SimulationLifecycleScheduler._miniqmt_capacity_residual_result_fields(item.run)
                    alert = capacity_fields.get("alert")
                    if isinstance(alert, dict):
                        alerts.append(alert)
                    processed.append(
                        {
                            "binding_id": item.binding_id,
                            "strategy_id": item.strategy_id,
                            "broker_backend": item.broker_backend.value,
                            "status": item.status,
                            "run_id": item.run.run_id if item.run else None,
                            "execution_plan_id": item.execution_plan.plan_id if item.execution_plan else None,
                            "data_source": item.data_source or self._data_source,
                            "error": item.error,
                            "lifecycle_diagnostic": item.lifecycle_diagnostic,
                            **capacity_fields,
                        }
                    )
                for terminalized in tick.stale_run_results:
                    alert = terminalized.get("alert")
                    if isinstance(alert, dict):
                        alerts.append(alert)
                    recovery_error = terminalized.get("error")
                    if terminalized.get("terminalization_succeeded") is False and isinstance(recovery_error, dict):
                        result["errors"].append(
                            {
                                **recovery_error,
                                "stage": terminalized.get("stage"),
                                "reason_code": terminalized.get("reason_code"),
                            }
                        )
                result["processed"] = processed
                result["terminalized_runs"] = list(tick.stale_run_results)
                result["alerts"] = alerts
                result["summary"] = {
                    "total_bindings": tick.total_bindings,
                    "planned_count": tick.planned_count,
                    "reused_count": tick.reused_count,
                    "submitted_count": tick.submitted_count,
                    "failed_count": tick.failed_count,
                    "retired_package_skipped_count": sum(
                        1 for item in tick.results if item.status == "SKIPPED_RETIRED_PACKAGE"
                    ),
                    "stale_terminalized_count": tick.stale_terminalized_count,
                    "stale_recovery_failed_count": tick.stale_recovery_failed_count,
                    "succeeded_with_capacity_residual_count": sum(
                        1 for item in processed if item.get("succeeded_with_capacity_residual")
                    )
                    + sum(
                        1 for item in tick.stale_run_results if item.get("succeeded_with_capacity_residual")
                    ),
                    "capacity_residual_count": sum(
                        int(item.get("capacity_residual_count") or 0) for item in processed
                    )
                    + sum(
                        int(item.get("capacity_residual_count") or 0) for item in tick.stale_run_results
                    ),
                    "capacity_residual_failed_intents": sum(
                        int(item.get("capacity_residual_failed_intents") or 0) for item in processed
                    )
                    + sum(
                        int(item.get("capacity_residual_failed_intents") or 0) for item in tick.stale_run_results
                    ),
                }
            except Exception as exc:  # scheduler must expose failure, not crash silently
                payload = {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "context": getattr(exc, "context", None),
                }
                result["errors"].append(payload)
                logger.warning("Simulation runtime scheduler tick failed: %s", payload)
        return self._record_result(started_at=now, result=result)

    def _run_tca_eod_observation(
        self,
        *,
        terminalized_runs: tuple[Mapping[str, Any], ...],
        trade_date: date,
        as_of_time: datetime,
    ) -> list[dict[str, Any]]:
        """Keep observation failures out of scheduler/run/broker outcome handling."""

        try:
            outcomes = self._tca_eod_observation_hook.observe_post_reconciliation(
                lifecycle_scheduler=self.lifecycle_scheduler,
                terminalized_runs=terminalized_runs,
                trade_date=trade_date,
                as_of_time=as_of_time,
            )
            return [dict(item) for item in outcomes]
        except Exception as exc:  # noqa: BLE001 - hook isolation is a Phase 0A safety contract.
            payload = {
                "status": "FAILED",
                "reason_code": "ADAPTIVE_IS_TCA_EOD_HOOK_EXCEPTION",
                "stage": "TCA_EOD_SCHEDULER_SEAM",
                "error_type": type(exc).__name__,
            }
            logger.error("MiniQMT TCA EOD observation hook failed: %s", payload, exc_info=True)
            return [payload]

    def _emit_tca_observation_metrics(
        self, *, outcomes: list[dict[str, Any]], trade_date: date
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Metrics remain fail-isolated facts and cannot block scheduler completion."""

        try:
            emission = self._tca_observation_metrics_emitter.emit(
                outcomes=outcomes,
                trade_date=trade_date,
                source="simulation_runtime_background_scheduler",
            )
            return [dict(item) for item in emission.metrics], [dict(item) for item in emission.alerts]
        except Exception as exc:  # noqa: BLE001 - metric failure must not alter terminal reconciliation.
            payload = {
                "alert_type": "MINIQMT_TCA_OBSERVATION_METRIC_FAILURE",
                "severity": "WARNING",
                "reason_code": "ADAPTIVE_IS_TCA_METRIC_EMIT_FAILED",
                "stage": "TCA_EOD_METRIC",
                "trade_date": trade_date.isoformat(),
                "execution_gate": False,
                "observation_only": True,
                "error_type": type(exc).__name__,
            }
            logger.error("MiniQMT TCA observation metric emission failed: %s", payload, exc_info=True)
            return [], [payload]

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception:
                logger.exception("Simulation runtime scheduler run_once crashed")
            if self._stop_event.wait(timeout=self._interval_seconds):
                break

    def _window_decision(self, *, as_of_time: datetime, trade_date: date) -> dict[str, Any]:
        windows = SimulationLifecycleScheduler._compute_schedule_windows(trade_date=trade_date, as_of_time=as_of_time)
        active = next((item for item in windows if item["state"] == "ACTIVE"), None)
        if active is None:
            return {"window": None, "should_run": False, "submit": False, "reason": "outside_configured_windows"}
        action = str(active.get("action") or "")
        should_run = action in {"selection_evidence", "execution_plan", "submit", "eod_reconcile"}
        submit = bool(self._default_submit and action == "submit")
        return {"window": active, "should_run": should_run, "submit": submit, "reason": action}

    def _trading_day_status(self, *, trade_date: date) -> dict[str, Any]:
        service = self._trading_calendar_service
        status_method = getattr(service, "status", None)
        if callable(status_method):
            raw_status = dict(status_method(as_of_date=trade_date))
            if "is_trading_day" not in raw_status:
                raise DataUnavailableError(
                    "simulation runtime scheduler trading calendar status is missing is_trading_day",
                    context={
                        "reason_code": "SIMULATION_RUNTIME_TRADING_CALENDAR_STATUS_INVALID",
                        "trade_date": trade_date.isoformat(),
                        "service": type(service).__name__,
                        "status_keys": sorted(str(key) for key in raw_status),
                    },
                )
            is_trading_day = raw_status.get("is_trading_day")
            if not isinstance(is_trading_day, bool):
                raise DataUnavailableError(
                    "simulation runtime scheduler trading calendar status has non-boolean is_trading_day",
                    context={
                        "reason_code": "SIMULATION_RUNTIME_TRADING_CALENDAR_STATUS_INVALID",
                        "trade_date": trade_date.isoformat(),
                        "service": type(service).__name__,
                        "is_trading_day_type": type(is_trading_day).__name__,
                        "is_trading_day_value": repr(is_trading_day),
                    },
                )
            raw_status.setdefault("as_of_date", trade_date.isoformat())
            raw_status["is_trading_day"] = is_trading_day
            if not raw_status["is_trading_day"] and not raw_status.get("next_trading_day"):
                raw_status["next_trading_day"] = self._next_trading_day_iso(trade_date)
            return {
                "schema_version": "simulation_runtime_trading_day_gate_v1",
                "service": type(service).__name__,
                "policy": "skip_non_trading_day_before_selection_planning_submit",
                **raw_status,
            }
        is_trading_day_method = getattr(service, "is_trading_day", None)
        if not callable(is_trading_day_method):
            raise DataUnavailableError(
                "simulation runtime scheduler trading calendar service lacks status/is_trading_day",
                context={
                    "reason_code": "SIMULATION_RUNTIME_TRADING_CALENDAR_METHOD_MISSING",
                    "trade_date": trade_date.isoformat(),
                    "service": type(service).__name__,
                },
            )
        is_trading_day = is_trading_day_method(trade_date)
        if not isinstance(is_trading_day, bool):
            raise DataUnavailableError(
                "simulation runtime scheduler is_trading_day returned non-boolean value",
                context={
                    "reason_code": "SIMULATION_RUNTIME_TRADING_CALENDAR_STATUS_INVALID",
                    "trade_date": trade_date.isoformat(),
                    "service": type(service).__name__,
                    "is_trading_day_type": type(is_trading_day).__name__,
                    "is_trading_day_value": repr(is_trading_day),
                },
            )
        return {
            "schema_version": "simulation_runtime_trading_day_gate_v1",
            "service": type(service).__name__,
            "policy": "skip_non_trading_day_before_selection_planning_submit",
            "ok": True,
            "as_of_date": trade_date.isoformat(),
            "is_trading_day": is_trading_day,
            "next_trading_day": None if is_trading_day else self._next_trading_day_iso(trade_date),
        }

    def _next_trading_day_iso(self, trade_date: date) -> str | None:
        next_method = getattr(self._trading_calendar_service, "next_trading_day", None)
        if callable(next_method):
            next_day = next_method(trade_date)
            return next_day.isoformat() if isinstance(next_day, date) else str(next_day)
        next_after_method = getattr(self._trading_calendar_service, "next_trading_day_after", None)
        if callable(next_after_method):
            next_day = next_after_method(trade_date)
            return next_day.isoformat() if isinstance(next_day, date) else str(next_day)
        return None

    def _record_result(self, *, started_at: datetime, result: dict[str, Any]) -> dict[str, Any]:
        result["completed_at"] = SimulationLifecycleScheduler._scheduler_now().isoformat()
        self._last_run_at = started_at
        self._last_result = result
        return result

    @staticmethod
    def _trade_date(now: datetime) -> date:
        raw = (os.getenv("SIMULATION_RUNTIME_SCHEDULER_TRADE_DATE") or "").strip()
        if raw:
            return date.fromisoformat(raw)
        return now.date()

    @staticmethod
    def _default_interval() -> int:
        raw = (os.getenv("SIMULATION_RUNTIME_SCHEDULER_INTERVAL_SEC") or "30").strip()
        try:
            value = int(raw)
        except ValueError:
            logger.warning(
                "Simulation runtime scheduler invalid interval; using fail-safe default",
                extra={
                    "reason_code": "SIMULATION_SCHEDULER_INTERVAL_INVALID",
                    "env_var": "SIMULATION_RUNTIME_SCHEDULER_INTERVAL_SEC",
                    "raw_value": raw,
                    "fallback_seconds": 30,
                },
            )
            return 30
        if value <= 0:
            logger.warning(
                "Simulation runtime scheduler non-positive interval; using fail-safe default",
                extra={
                    "reason_code": "SIMULATION_SCHEDULER_INTERVAL_NON_POSITIVE",
                    "env_var": "SIMULATION_RUNTIME_SCHEDULER_INTERVAL_SEC",
                    "raw_value": raw,
                    "parsed_value": value,
                    "fallback_seconds": 30,
                },
            )
            return 30
        return value

    @staticmethod
    def _default_limit() -> int:
        raw = (os.getenv("SIMULATION_RUNTIME_SCHEDULER_LIMIT") or "100").strip()
        try:
            value = int(raw)
        except ValueError:
            logger.warning(
                "Simulation runtime scheduler invalid limit; using fail-safe default",
                extra={
                    "reason_code": "SIMULATION_SCHEDULER_LIMIT_INVALID",
                    "env_var": "SIMULATION_RUNTIME_SCHEDULER_LIMIT",
                    "raw_value": raw,
                    "fallback_limit": 100,
                },
            )
            return 100
        if value < 1:
            logger.warning(
                "Simulation runtime scheduler non-positive limit; using fail-safe minimum",
                extra={
                    "reason_code": "SIMULATION_SCHEDULER_LIMIT_NON_POSITIVE",
                    "env_var": "SIMULATION_RUNTIME_SCHEDULER_LIMIT",
                    "raw_value": raw,
                    "parsed_value": value,
                    "fallback_limit": 1,
                },
            )
            return 1
        if value > 500:
            logger.warning(
                "Simulation runtime scheduler excessive limit; clamping to fail-safe maximum",
                extra={
                    "reason_code": "SIMULATION_SCHEDULER_LIMIT_TOO_LARGE",
                    "env_var": "SIMULATION_RUNTIME_SCHEDULER_LIMIT",
                    "raw_value": raw,
                    "parsed_value": value,
                    "fallback_limit": 500,
                },
            )
            return 500
        return value

    def _data_source_policy(self) -> dict[str, str]:
        return {
            "default": self._data_source,
            "local_sim_same_day": MinuteDataSource.TDX_REALTIME.value,
            "local_sim_historical": "persisted_portfolio_data_source",
            "miniqmt_sim": MinuteDataSource.MINIQMT_REALTIME.value,
        }

    def _trading_calendar_policy(self) -> dict[str, str]:
        return {
            "service": type(self._trading_calendar_service).__name__,
            "source_of_truth": "TradingCalendarStatusService",
            "timezone": SCHEDULER_TZ_NAME,
            "non_trading_day": "skip_no_selection_plan_submit",
            "calendar_unavailable": "fail_closed_no_submit",
        }

    @staticmethod
    def _env_flag(name: str, *, default: bool) -> bool:
        raw = (os.getenv(name) or "").strip().lower()
        if not raw:
            return default
        return raw in {"1", "true", "yes", "y", "on"}


simulation_lifecycle_background_scheduler = SimulationLifecycleBackgroundScheduler(
    lifecycle_scheduler=simulation_lifecycle_scheduler
)
