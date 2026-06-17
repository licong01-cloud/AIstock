"""Scheduler entry point for unified LocalSim and MiniQMT simulation runs.

The scheduler is intentionally broker-neutral until ``submit=True`` is passed.
It drives StrategyRuntimeRelease -> DailySelectionEvidence -> ExecutionPlan for
eligible SimulationReleaseBinding rows and reuses persisted plans on restart so
that a backend tick cannot duplicate orders.
"""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any, Protocol

import psycopg2.extras

from backend.db.pg_pool import get_conn
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
    STATUS_OPEN_LIKE,
    new_id as new_qmt_id,
)
from backend.services.qmt_strategy_ledger.sync_service import QmtStrategyLedgerSyncService
from backend.services.selection_center.models import SelectionMode, SignalSnapshot
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError
from backend.services.trading_core.models import AccountSnapshot, OrderSide, PositionLot, RunStatus

from .lifecycle import SimulationExecutionResult, SimulationLifecycleOrchestrator, SimulationPlanBuildResult
from .models import (
    DailySelectionEvidence,
    ExecutionPlan,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
    SimulationDailyRun,
    SimulationDailyRunStatus,
    SimulationReleaseBinding,
    StrategyRuntimeRelease,
    canonical_json_sha256,
)
from .repository import InMemorySimulationRuntimeRepository, SimulationRuntimeRepository
from .selection import StrategyPackageSelectionResult, StrategyPackageSelectionService
from .service import StrategyRuntimeReleaseService
from .performance import StrategyPerformanceProjectionService
from .tail import TailHandlingPolicyService


DEFAULT_SCHEDULER_APPROVAL_STATES = (
    SimulationBindingApprovalState.SIM_VALIDATING,
    SimulationBindingApprovalState.SIM_PASSED,
    SimulationBindingApprovalState.LIVE_APPROVAL_PENDING,
    SimulationBindingApprovalState.LIVE_APPROVED,
)

DEFAULT_SCHEDULER_WINDOWS = (
    {"window_id": "pre_open", "label": "盘前", "start": "08:50", "end": "09:10", "action": "readiness"},
    {"window_id": "selection", "label": "选股", "start": "09:10", "end": "09:20", "action": "selection_evidence"},
    {"window_id": "planning", "label": "调仓", "start": "09:20", "end": "09:25", "action": "execution_plan"},
    {"window_id": "execution", "label": "盘中/尾盘", "start": "09:25", "end": "15:00", "action": "submit"},
)

logger = logging.getLogger("aistock.simulation_runtime.scheduler")

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

_LOCALSIM_ROLL_FORWARD_CREATED_BY = "simulation_lifecycle_scheduler.localsim_roll_forward"
_MINIQMT_ROLL_FORWARD_CREATED_BY = "simulation_lifecycle_scheduler.miniqmt_roll_forward"
_LOCALSIM_CASH_FIT_BUY_BUFFER_RATIO = 1.02
_LOCALSIM_CASH_FIT_SELL_PROCEEDS_BUFFER_RATIO = 0.98
_LOCALSIM_DEFAULT_OPEN_COST = 0.000095
_LOCALSIM_DEFAULT_CLOSE_COST = 0.000595
_LOCALSIM_DEFAULT_MIN_FEE = 5.0


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


@dataclass(frozen=True)
class LocalSimPersistenceResult:
    payload: dict[str, Any]
    positions: dict[str, PositionLot]
    marks: dict[str, float]
    cash: float


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
        if binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM:
            return self._load_local_sim_context(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
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
        market_data_source = self._resolve_local_sim_market_data_source(portfolio=portfolio, trade_date=trade_date)
        pre_trade_tradability = self._load_pre_trade_tradability(
            symbols=list(positions),
            trade_date=trade_date,
            require_realtime_quote=(
                market_data_source == MinuteDataSource.TDX_REALTIME
                and self._position_loader is None
            ),
        )
        manifest = getattr(portfolio, "frozen_manifest", None)
        self._validate_manifest_identity(
            manifest=manifest,
            runtime_release=runtime_release,
            binding=binding,
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
            context_diagnostics={
                "localsim_tplus1_settlement": settlement_diagnostics,
                "pre_trade_tradability": self._pre_trade_tradability_diagnostics(pre_trade_tradability),
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
        pre_trade_tradability = self._load_pre_trade_tradability(
            symbols=list(positions),
            trade_date=trade_date,
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
            },
            cash=float(account.cash),
            frozen_cash=float(account.frozen_cash),
            realized_pnl=float(account.realized_pnl),
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
    ) -> dict[str, dict[str, Any]]:
        require_realtime_quote = self._position_loader is None and (
            (binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM and trade_date == date.today())
            or (
                binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM
                and (market_data_source == MinuteDataSource.TDX_REALTIME.value or trade_date == date.today())
            )
        )
        return self._load_pre_trade_tradability(
            symbols=symbols,
            trade_date=trade_date,
            require_realtime_quote=require_realtime_quote,
        )

    def _load_pre_trade_tradability(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        require_realtime_quote: bool,
    ) -> dict[str, dict[str, Any]]:
        if not require_realtime_quote and not self._pre_trade_tradability_provider_injected:
            return {}
        if self._position_loader is not None and not self._pre_trade_tradability_provider_injected:
            return {}
        loader = getattr(self._pre_trade_tradability_provider, "get_statuses", None)
        if not callable(loader):
            raise DataUnavailableError(
                "pre-trade tradability provider must expose get_statuses",
                context={"provider": type(self._pre_trade_tradability_provider).__name__},
            )
        try:
            raw = loader(symbols, trade_date, require_realtime_quote=require_realtime_quote)
        except TypeError:
            raw = loader(symbols, trade_date)
        if not isinstance(raw, dict):
            raise DataUnavailableError(
                "pre-trade tradability provider returned invalid payload",
                context={"provider": type(self._pre_trade_tradability_provider).__name__},
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
    ) -> MinuteDataSource:
        data_source = getattr(portfolio, "data_source", MinuteDataSource.DB_HISTORICAL)
        if not isinstance(data_source, MinuteDataSource):
            data_source = MinuteDataSource(str(data_source))
        if trade_date == date.today():
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
    mode = (os.getenv("SIMULATION_RUNTIME_CONTEXT_PROVIDER") or "").strip().lower()
    production_enabled = _env_flag("ENABLE_SIMULATION_RUNTIME_PRODUCTION_PROVIDER", default=False)
    if mode in {"production", "prod"} or production_enabled:
        provider: SimulationRunContextProvider = ProductionSimulationRunContextProvider()
    else:
        provider = FailFastSimulationRunContextProvider()
    return SimulationLifecycleScheduler(repository=repository, context_provider=provider)


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
        return len(self.stale_run_results)


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
    ) -> None:
        self.repository = repository or SimulationRuntimeRepository()
        self.selection_service = selection_service or StrategyPackageSelectionService(repository=self.repository)
        self.orchestrator = orchestrator or SimulationLifecycleOrchestrator(repository=self.repository)
        self.context_provider = context_provider or FailFastSimulationRunContextProvider()
        self.performance_service = performance_service or StrategyPerformanceProjectionService()

    def status(self) -> dict[str, Any]:
        provider_status = _context_provider_status(self.context_provider)
        return {
            "ok": True,
            "scheduler": "simulation_lifecycle_scheduler",
            "autostart": False,
            "default_submit": False,
            "approval_states": [state.value for state in DEFAULT_SCHEDULER_APPROVAL_STATES],
            "manual_tick_endpoint_enabled": True,
            "scheduler_control_api_enabled": False,
            "context_provider": provider_status,
            "context_provider_mode": provider_status.get("provider_mode"),
            "schedule_windows": list(DEFAULT_SCHEDULER_WINDOWS),
            "restart_recovery_mode": "persisted_state_only",
            "window_orchestration": {
                "pre_open": "readiness",
                "selection": "daily_selection_evidence",
                "planning": "execution_plan",
                "execution": "submit_and_reconcile",
            },
        }

    def run_once(
        self,
        *,
        trade_date: date,
        data_source: str,
        limit: int = 100,
        broker_backend: SimulationBrokerBackend | str | None = None,
        strategy_id: str | None = None,
        release_id: str | None = None,
        approval_states: tuple[SimulationBindingApprovalState, ...] | None = DEFAULT_SCHEDULER_APPROVAL_STATES,
        submit: bool = False,
        mode: str = "SIM",
        as_of_time: datetime | None = None,
        created_by: str = "simulation_lifecycle_scheduler",
        raise_on_error: bool = False,
    ) -> SimulationSchedulerRunOnceResult:
        if limit <= 0:
            raise ValueError("limit must be positive")
        stale_run_results = self._terminalize_stale_miniqmt_active_runs(
            trade_date=trade_date,
            broker_backend=broker_backend,
            strategy_id=strategy_id,
            limit=limit,
        )
        bindings = self.repository.list_simulation_release_bindings(
            strategy_id=strategy_id,
            release_id=release_id,
            broker_backend=broker_backend,
            approval_states=approval_states,
            active_on=trade_date,
            limit=limit,
        )
        bindings = self._with_unattended_roll_forward_bindings(
            bindings=bindings,
            trade_date=trade_date,
            limit=limit,
            broker_backend=broker_backend,
            strategy_id=strategy_id,
            release_id=release_id,
            approval_states=approval_states,
        )
        results: list[SimulationSchedulerBindingResult] = []
        selection_cache: dict[tuple[Any, ...], StrategyPackageSelectionResult | BaseException] = {}
        shared_selection_keys = self._shared_selection_cache_keys(
            bindings=bindings,
            trade_date=trade_date,
            data_source=data_source,
        )
        for binding in bindings:
            try:
                results.append(
                    self._run_binding(
                        binding=binding,
                        trade_date=trade_date,
                        data_source=data_source,
                        submit=submit,
                        mode=mode,
                        created_by=created_by,
                        selection_cache=selection_cache,
                        shared_selection_keys=shared_selection_keys,
                    )
                )
            except (DataUnavailableError, RuntimeConfigInvalidError) as exc:
                if raise_on_error:
                    raise
                results.append(
                    SimulationSchedulerBindingResult(
                        binding_id=binding.binding_id,
                        strategy_id=binding.strategy_id,
                        broker_backend=binding.broker_backend,
                        status="FAILED",
                        error={
                            "type": type(exc).__name__,
                            "message": str(exc),
                            "context": getattr(exc, "context", None),
                        },
                        data_source=self._effective_market_data_source_for_binding(
                            binding=binding,
                            trade_date=trade_date,
                            default_data_source=data_source,
                        ),
                    )
                )
        return SimulationSchedulerRunOnceResult(
            trade_date=trade_date,
            data_source=data_source,
            submit=submit,
            total_bindings=len(bindings),
            results=tuple(results),
            stale_run_results=tuple(stale_run_results),
            as_of_time=as_of_time,
            schedule_windows=self._compute_schedule_windows(trade_date=trade_date, as_of_time=as_of_time),
        )

    def _terminalize_stale_miniqmt_active_runs(
        self,
        *,
        trade_date: date,
        broker_backend: SimulationBrokerBackend | str | None,
        strategy_id: str | None,
        limit: int,
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
                had_side_effect = bool(run.run_payload_json.get("broker_called") or run.run_payload_json.get("qmt_batch_id"))
                next_status = (
                    SimulationDailyRunStatus.FAILED_RETRYABLE
                    if had_side_effect
                    else SimulationDailyRunStatus.CANCELLED
                )
                evidence = {
                    "schema_version": "miniqmt_stale_active_run_terminalization_v1",
                    "reason": (
                        "stale_historical_miniqmt_run_with_broker_side_effect"
                        if had_side_effect
                        else "stale_historical_miniqmt_run_without_broker_side_effect"
                    ),
                    "scheduler_trade_date": trade_date.isoformat(),
                    "stale_trade_date": run.trade_date.isoformat(),
                    "previous_status": run.status.value,
                    "had_broker_side_effect": had_side_effect,
                    "terminalized_at": datetime.now(UTC).isoformat(),
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
                terminalized.append(
                    {
                        "run_id": updated.run_id,
                        "trade_date": updated.trade_date.isoformat(),
                        "strategy_id": updated.strategy_id,
                        "previous_status": run.status.value,
                        "status": updated.status.value,
                        "reason": evidence["reason"],
                    }
                )
        return terminalized[:limit]

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
    ) -> list[SimulationReleaseBinding]:
        if release_id is not None:
            return bindings

        remaining_slots = limit - len(bindings)
        if remaining_slots <= 0:
            return bindings
        existing_keys = {(item.strategy_id, item.broker_backend) for item in bindings}
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
    ) -> SimulationReleaseBinding:
        source_release = self.repository.get_strategy_runtime_release(source.release_id)
        release_service = StrategyRuntimeReleaseService(repository=self.repository)
        created_by = self._roll_forward_created_by(source.broker_backend)
        release_metadata = self._roll_forward_release_metadata(
            source_release=source_release,
            source_binding=source,
            trade_date=trade_date,
        )
        validation_evidence = self._roll_forward_validation_evidence(
            source_release=source_release,
            source_binding=source,
            trade_date=trade_date,
        )
        new_release = release_service.create_release(
            package_id=source_release.package_id,
            manifest_sha256=source_release.manifest_sha256,
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
    ) -> SimulationSchedulerBindingResult:
        runtime_release = self.repository.get_strategy_runtime_release(binding.release_id)
        existing = self.repository.get_simulation_daily_run_by_key(
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
            trade_date=trade_date,
        )
        if existing is not None and existing.execution_plan_id:
            if self._should_rebuild_localsim_plan_after_side_effect_free_failure(binding=binding, run=existing):
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
                )
            return self._existing_plan_result(
                binding=binding,
                run=existing,
                trade_date=trade_date,
                data_source=data_source,
                submit=submit,
                mode=mode,
            )

        context = self.context_provider.load_context(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
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
        )
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

        sync_result = self._sync_before_submit(binding=binding, run=build_result.run, context=context)
        build_result, residual_only = self._prepare_localsim_build_result_for_submit(
            binding=binding,
            build_result=build_result,
            context=context,
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
        execution = self.orchestrator.submit_execution_plan(
            build_result=build_result,
            local_broker=context.local_broker,
            managed_order_service=context.managed_order_service,
            mode=mode,
            price_by_symbol=context.price_by_symbol or context.current_prices,
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
        tail_result = self._handle_tail_after_submit(binding=binding, run=execution.run, execution=execution, context=context)
        reconciliation = self._reconcile_after_submit(binding=binding, run=execution.run, context=context)
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
    ) -> SimulationSchedulerBindingResult:
        context = self.context_provider.load_context(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
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
        execution = self.orchestrator.submit_execution_plan(
            build_result=build_result,
            local_broker=context.local_broker,
            managed_order_service=context.managed_order_service,
            mode=mode,
            price_by_symbol=context.price_by_symbol or context.current_prices,
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
        tail_result = self._handle_tail_after_submit(binding=binding, run=execution.run, execution=execution, context=context)
        reconciliation = self._reconcile_after_submit(binding=binding, run=execution.run, context=context)
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
    ) -> SimulationSchedulerBindingResult:
        context = self.context_provider.load_context(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
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

        sync_result = self._sync_before_submit(binding=binding, run=build_result.run, context=context)
        build_result, residual_only = self._prepare_localsim_build_result_for_submit(
            binding=binding,
            build_result=build_result,
            context=context,
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
        execution = self.orchestrator.submit_execution_plan(
            build_result=build_result,
            local_broker=context.local_broker,
            managed_order_service=context.managed_order_service,
            mode=mode,
            price_by_symbol=context.price_by_symbol or context.current_prices,
        )
        local_persistence = self._persist_local_sim_execution_result(
            binding=binding,
            run=execution.run,
            execution=execution,
            context=context,
        )
        tail_result = self._handle_tail_after_submit(binding=binding, run=execution.run, execution=execution, context=context)
        reconciliation = self._reconcile_after_submit(binding=binding, run=execution.run, context=context)
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
        if self._should_reconcile_existing_miniqmt_run(binding=binding, run=run, submit=submit):
            context = self.context_provider.load_context(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
            )
            sync_result = self._sync_before_submit(binding=binding, run=run, context=context)
            reconciliation = self._reconcile_after_submit(binding=binding, run=run, context=context)
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
        if self._should_submit_existing_plan(binding=binding, run=run, plan=plan, submit=submit):
            runtime_release = self.repository.get_strategy_runtime_release(binding.release_id)
            try:
                context = self.context_provider.load_context(
                    runtime_release=runtime_release,
                    binding=binding,
                    trade_date=trade_date,
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
            execution = self.orchestrator.submit_persisted_execution_plan(
                run=run,
                binding=binding,
                execution_plan=plan,
                local_broker=context.local_broker,
                managed_order_service=context.managed_order_service,
                mode=mode,
                price_by_symbol=context.price_by_symbol or context.current_prices,
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
            tail_result = self._handle_tail_after_submit(binding=binding, run=execution.run, execution=execution, context=context)
            reconciliation = self._reconcile_after_submit(binding=binding, run=execution.run, context=context)
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
        diagnostic = {
            "schema_version": "localsim_pre_submit_retry_diagnostics_v1",
            "stage": self._localsim_pre_submit_failure_stage(exc),
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

    @staticmethod
    def _execution_plan_side_counts(plan: ExecutionPlan) -> dict[str, int]:
        return {
            "intent_count": len(plan.intents),
            "buy_intent_count": sum(1 for intent in plan.intents if intent.side == OrderSide.BUY),
            "sell_intent_count": sum(1 for intent in plan.intents if intent.side == OrderSide.SELL),
        }

    @staticmethod
    def _localsim_pre_submit_failure_stage(exc: BaseException) -> str:
        message = str(exc).lower()
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
        prepared_plan, fit_payload = self._cash_fit_localsim_execution_plan(plan=plan, context=context)
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
        plan: ExecutionPlan,
        context: SimulationRunContext,
    ) -> tuple[ExecutionPlan, dict[str, Any]]:
        cash = float(context.cash if context.cash is not None else 0.0)
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

    @staticmethod
    def _should_rebuild_localsim_plan_after_side_effect_free_failure(
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
    ) -> bool:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
            return False
        if run.status != SimulationDailyRunStatus.FAILED_RETRYABLE:
            return False
        if bool(run.run_payload_json.get("broker_called")):
            return False
        failure = run.run_payload_json.get("submit_failure")
        if not isinstance(failure, dict) or failure.get("stage") != "LOCAL_SIM_SUBMIT_FAILED":
            return False
        context = failure.get("context") if isinstance(failure.get("context"), dict) else {}
        text = " ".join(
            str(item or "")
            for item in (
                failure.get("message"),
                context.get("cause"),
                context.get("cause_code"),
            )
        ).lower()
        return "insufficient cash" in text

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
        if status not in {"PREFLIGHT_FAILED", "FAILED", "PARTIAL"}:
            return False
        try:
            failed = int(batch.get("failed", payload.get("failed_intents", 0)) or 0)
        except (TypeError, ValueError):
            failed = 0
        return failed > 0

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
            selection = self.selection_service.run_selection(
                package_ids=[binding.package_id],
                mode=SelectionMode.SINGLE_PACKAGE,
                trade_date=trade_date,
                data_source=data_source,
                runtime_config=StrategyPackageSelectionService.release_selection_runtime_config(runtime_release),
                runtime_release=runtime_release,
                created_by=created_by,
            )
        except Exception as exc:
            if selection_cache is not None:
                selection_cache[cache_key] = exc
            raise
        if selection_cache is not None:
            selection_cache[cache_key] = selection
        return selection

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
        elif local_persistence is not None:
            projection = self.performance_service.project_strategy(
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                initial_capital=float(binding.capital_allocation),
                cash=float(local_persistence.cash),
                frozen_cash=0.0,
                realized_pnl=float(context.realized_pnl),
                positions=local_persistence.positions,
                marks=local_persistence.marks,
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
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
            return None
        if execution.status != "SUBMITTED":
            return None
        try:
            broker_result = execution.broker_result
            snapshot = getattr(broker_result, "execution_snapshot", None)
            if snapshot is None:
                raise DataUnavailableError(
                    "LocalSim submit returned no execution snapshot for durable persistence",
                    context={
                        "run_id": run.run_id,
                        "strategy_id": binding.strategy_id,
                        "binding_id": binding.binding_id,
                        "plan_id": execution.execution_plan.plan_id,
                    },
                )
            orders = tuple(getattr(snapshot, "orders", ()) or ())
            fills = tuple(getattr(snapshot, "fills", ()) or ())
            events = tuple(getattr(snapshot, "events", ()) or ())
            snapshot_cash_entries = tuple(getattr(snapshot, "cash_entries", ()) or ())
            orders, fills, events, cash_entries = self._filter_local_sim_snapshot_by_plan(
                execution=execution,
                orders=orders,
                fills=fills,
                events=events,
                cash_entries=snapshot_cash_entries,
            )
            positions = dict(getattr(snapshot, "positions", {}) or {})
            account = getattr(snapshot, "account", None)
            self._validate_local_sim_snapshot_for_success(
                run=run,
                execution=execution,
                orders=orders,
                fills=fills,
                cash_entries=cash_entries,
            )
            marks = self._local_sim_position_marks(
                positions=positions,
                context=context,
                execution=execution,
            )
            cash = float(getattr(account, "cash")) if account is not None else self._cash_after_local_sim(cash_entries, context)
            snapshot_time = self._local_sim_snapshot_time(fills=fills, events=events, run=run)
            market_value = sum(int(position.quantity) * marks[position.symbol] for position in positions.values())
            account_snapshot = AccountSnapshot(
                portfolio_id=str(context.portfolio_id or execution.execution_plan.portfolio_id),
                cash=cash,
                market_value=market_value,
                nav=cash + market_value,
                snapshot_time=snapshot_time,
            )
            paper_repository = self._paper_repository_for_local_sim(
                binding=binding,
                run=run,
                context=context,
            )
            self._ensure_local_sim_paper_run(
                repository=paper_repository,
                run=run,
                context=context,
            )
            for order in orders:
                paper_repository.save_order(run.run_id, order)
            for fill in fills:
                paper_repository.save_fill(run.run_id, fill)
            for event in events:
                paper_repository.save_order_event(run.run_id, event)
            for entry in cash_entries:
                paper_repository.save_cash_entry(run.run_id, entry)
            paper_repository.save_positions(
                run_id=run.run_id,
                trade_date=run.trade_date,
                positions=list(positions.values()),
                prices=marks,
            )
            paper_repository.save_daily_snapshot(
                run_id=run.run_id,
                trade_date=run.trade_date,
                snapshot=account_snapshot,
                metadata={
                    "source": "simulation_runtime_local_sim",
                    "simulation_run_id": run.run_id,
                    "execution_plan_id": execution.execution_plan.plan_id,
                    "order_count": len(orders),
                    "fill_count": len(fills),
                    "cash_ledger_count": len(cash_entries),
                    "position_count": len(positions),
                },
            )
            cash_fit_residual = self._local_sim_cash_fit_residual_payload(run)
            paper_repository.save_run_event(
                run_id=run.run_id,
                event_type="RUN_CAPACITY_RESIDUAL_SKIPPED" if cash_fit_residual else "RUN_SUCCEEDED",
                message=(
                    "simulation runtime LocalSim execution persisted with capacity residual skipped"
                    if cash_fit_residual
                    else "simulation runtime LocalSim execution persisted to Paper v2"
                ),
                context={
                    "source": "simulation_runtime_local_sim",
                    "simulation_run_id": run.run_id,
                    "execution_plan_id": execution.execution_plan.plan_id,
                    "order_count": len(orders),
                    "fill_count": len(fills),
                    "cash_ledger_count": len(cash_entries),
                    "position_count": len(positions),
                    "snapshot_time": snapshot_time.isoformat(),
                    "local_sim_cash_fit": cash_fit_residual,
                },
            )
            paper_repository.update_run_status(
                paper_repository.get_run(run.run_id),
                RunStatus.FAILED if cash_fit_residual else RunStatus.SUCCEEDED,
                error={
                    "code": "LOCALSIM_CAPACITY_RESIDUAL_SKIPPED",
                    "message": "LocalSim skipped non-executable BUY residual after cash-fit planning",
                    "context": cash_fit_residual,
                }
                if cash_fit_residual
                else None,
            )
            next_status = (
                SimulationDailyRunStatus.FAILED_TERMINAL
                if cash_fit_residual
                else SimulationDailyRunStatus.SUCCEEDED
            )
            local_sim_persistence_payload = {
                "schema_version": "local_sim_persistence_v1",
                "status": "PERSISTED_WITH_CAPACITY_RESIDUAL" if cash_fit_residual else "PERSISTED",
                "paper_v2_run_id": run.run_id,
                "order_count": len(orders),
                "fill_count": len(fills),
                "order_event_count": len(events),
                "cash_ledger_count": len(cash_entries),
                "position_count": len(positions),
                "snapshot_time": snapshot_time.isoformat(),
                "cash": cash,
                "nav": account_snapshot.nav,
            }
            payload_patch = {
                "local_sim_persistence": local_sim_persistence_payload,
                "last_stage": next_status.value,
            }
            if cash_fit_residual:
                payload_patch["local_sim_capacity_residual_terminalization"] = {
                    "schema_version": "localsim_capacity_residual_terminalization_v1",
                    "reason": "cash_fit_skipped_non_executable_buy_residual",
                    "status": next_status.value,
                    "skipped_buy_count": int(cash_fit_residual.get("skipped_buy_count") or 0),
                    "prepared_intent_count": int(cash_fit_residual.get("prepared_intent_count") or 0),
                    "terminalized_at": datetime.now(UTC).isoformat(),
                }
            self.repository.update_simulation_daily_run(
                run.run_id,
                status=next_status,
                payload_patch=payload_patch,
                payload_unset=("submit_failure", "local_sim_retry_diagnostics"),
            )
            return LocalSimPersistenceResult(
                payload={
                    "order_count": len(orders),
                    "fill_count": len(fills),
                    "cash_ledger_count": len(cash_entries),
                    "position_count": len(positions),
                    "cash": cash,
                    "nav": account_snapshot.nav,
                },
                positions=positions,
                marks=marks,
                cash=cash,
            )
        except Exception as exc:
            if not isinstance(exc, DataUnavailableError):
                exc = DataUnavailableError(
                    "LocalSim execution side effects could not be persisted durably",
                    context={
                        "run_id": run.run_id,
                        "strategy_id": binding.strategy_id,
                        "binding_id": binding.binding_id,
                        "plan_id": execution.execution_plan.plan_id,
                        "cause": str(exc),
                    },
                )
            stage = self._local_sim_persistence_failure_stage(exc)
            self.orchestrator.mark_submit_failure(run=run, stage=stage, exc=exc)
            raise exc

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
        message = str(exc)
        if "no execution snapshot" in message:
            return "LOCAL_SIM_PERSISTENCE_SNAPSHOT_MISSING"
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
    ) -> dict[str, float]:
        marks = SimulationLifecycleScheduler._performance_marks(context)
        for intent in execution.execution_plan.intents:
            if intent.symbol not in marks and intent.price_policy.get("reference_price") is not None:
                marks[intent.symbol] = float(intent.price_policy["reference_price"])
            if intent.symbol not in marks and intent.price_policy.get("limit_price") is not None:
                marks[intent.symbol] = float(intent.price_policy["limit_price"])
        missing = sorted(symbol for symbol in positions if symbol not in marks)
        if missing:
            raise DataUnavailableError(
                "LocalSim persistence requires mark prices for all persisted positions",
                context={"symbols": missing, "plan_id": execution.execution_plan.plan_id},
            )
        return {symbol: float(marks[symbol]) for symbol in positions}

    @staticmethod
    def _cash_after_local_sim(cash_entries: tuple[Any, ...], context: SimulationRunContext) -> float:
        if cash_entries:
            return float(getattr(cash_entries[-1], "cash_after"))
        if context.cash is not None:
            return float(context.cash)
        raise DataUnavailableError("LocalSim persistence requires account cash or cash ledger entries")

    @staticmethod
    def _local_sim_snapshot_time(*, fills: tuple[Any, ...], events: tuple[Any, ...], run: SimulationDailyRun) -> datetime:
        if fills:
            return max(getattr(fill, "trade_time") for fill in fills)
        if events:
            return max(getattr(event, "event_time") for event in events)
        return datetime.combine(run.trade_date, time(15, 0), tzinfo=UTC)

    @staticmethod
    def _performance_marks(context: SimulationRunContext) -> dict[str, float]:
        marks = dict(context.current_prices or {})
        if context.price_by_symbol:
            marks.update({symbol: float(price) for symbol, price in context.price_by_symbol.items()})
        return marks

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
                or (
                    run.status != SimulationDailyRunStatus.FAILED_RETRYABLE
                    and SimulationLifecycleScheduler._mini_qmt_batch_has_retryable_buy_residual(run.run_payload_json)
                )
            )
        )

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
    def _mini_qmt_batch_has_terminal_capacity_residual(payload: dict[str, Any]) -> bool:
        summary = SimulationLifecycleScheduler._mini_qmt_batch_residual_summary(payload)
        return (
            bool(summary.get("noncompensating_residual"))
            and int(summary.get("capacity_residual_count") or 0) > 0
            and int(summary.get("dependent_buy_count") or 0) == 0
        )

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
        payload = {
            **payload,
            "strategy_scope": strategy_scope,
            "run_status_gate": run_status_gate,
            "submit_result_gate": submit_result_gate,
            "qmt_batch_residual_summary": batch_residual_summary,
            "open_order_evidence": open_order_evidence,
            "side_effect_evidence": side_effect_evidence,
        }
        if submit_result_gate["status"] == "SUCCEEDED":
            next_status = SimulationDailyRunStatus.SUCCEEDED
        elif submit_result_gate["status"] == "PENDING":
            next_status = SimulationDailyRunStatus.INTRADAY_RUNNING
        else:
            next_status = SimulationDailyRunStatus.FAILED_RETRYABLE
        self.repository.update_simulation_daily_run(
            run.run_id,
            status=next_status,
            payload_patch={
                "last_stage": next_status.value,
                "reconcile_after_submit": payload,
            },
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
        terminal_capacity_residual = (
            bool(batch_residual_summary.get("noncompensating_residual"))
            and int(batch_residual_summary.get("capacity_residual_count") or 0) > 0
            and int(batch_residual_summary.get("dependent_buy_count") or 0) == 0
        )
        open_order_count = int(open_order_evidence.get("open_order_count") or 0)
        broker_side_effect_count = int(side_effect_evidence.get("broker_side_effect_count") or 0)
        if run_status_gate.get("status") != "SUCCEEDED":
            status = "blocked"
            reason = "miniqmt_reconciliation_run_status_gate_not_succeeded"
        elif open_order_count > 0:
            status = "PENDING"
            reason = "miniqmt_open_orders_pending_after_reconciliation"
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
            "open_order_count": open_order_count,
            "pending_open_orders": open_order_count > 0,
            "broker_side_effect_count": broker_side_effect_count,
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
            if getattr(order, "order_status", None) == STATUS_OPEN_LIKE
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
    ) -> SimulationPlanBuildResult:
        evidence = selection.evidence_by_package[binding.package_id]
        candidates = selection.package_results.get(binding.package_id, [])
        pre_trade_tradability = self._pre_trade_tradability_for_planning(
            binding=binding,
            trade_date=trade_date,
            context=context,
            candidate_symbols=[candidate.symbol for candidate in candidates],
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
        return self.orchestrator.build_execution_plan(
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
            created_by=created_by,
        )

    def _pre_trade_tradability_for_planning(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        context: SimulationRunContext,
        candidate_symbols: list[str],
    ) -> dict[str, dict[str, Any]]:
        symbols = sorted({*context.current_positions.keys(), *[str(symbol).strip() for symbol in candidate_symbols if str(symbol).strip()]})
        statuses = {str(symbol): dict(status) for symbol, status in (context.pre_trade_tradability or {}).items()}
        missing = [symbol for symbol in symbols if symbol not in statuses]
        loader = getattr(self.context_provider, "load_pre_trade_tradability", None)
        if missing and callable(loader):
            statuses.update(
                loader(
                    symbols=missing,
                    trade_date=trade_date,
                    binding=binding,
                    market_data_source=context.market_data_source,
                )
            )
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
        current_time = as_of_time.time().replace(second=0, microsecond=0) if as_of_time is not None else None
        windows: list[dict[str, Any]] = []
        for window in DEFAULT_SCHEDULER_WINDOWS:
            start = datetime.combine(trade_date, time.fromisoformat(window["start"]))
            end = datetime.combine(trade_date, time.fromisoformat(window["end"]))
            state = "PENDING"
            if current_time is not None:
                if current_time < start.time():
                    state = "UPCOMING"
                elif start.time() <= current_time < end.time():
                    state = "ACTIVE"
                else:
                    state = "COMPLETED"
            windows.append(
                {
                    **window,
                    "trade_date": trade_date.isoformat(),
                    "state": state,
                    "start_at": start.isoformat(),
                    "end_at": end.isoformat(),
                }
            )
        return tuple(windows)


simulation_lifecycle_scheduler = build_simulation_lifecycle_scheduler_from_env()


class SimulationLifecycleBackgroundScheduler:
    """Opt-in unattended scheduler wrapper with trading-window orchestration."""

    def __init__(
        self,
        *,
        lifecycle_scheduler: SimulationLifecycleScheduler | None = None,
    ) -> None:
        self.lifecycle_scheduler = lifecycle_scheduler or SimulationLifecycleScheduler()
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
        }

    def run_once(self, *, as_of_time: datetime | None = None) -> dict[str, Any]:
        now = as_of_time or datetime.now()
        trade_date = self._trade_date(now)
        decision = self._window_decision(as_of_time=now, trade_date=trade_date)
        result: dict[str, Any] = {
            "started_at": now.isoformat(),
            "trade_date": trade_date.isoformat(),
            "data_source": self._data_source,
            "data_source_policy": self._data_source_policy(),
            "window": decision["window"],
            "should_run": decision["should_run"],
            "submit": decision["submit"],
            "processed": [],
            "errors": [],
        }
        if decision["should_run"]:
            try:
                tick = self.lifecycle_scheduler.run_once(
                    trade_date=trade_date,
                    data_source=self._data_source,
                    limit=self._limit,
                    submit=bool(decision["submit"]),
                    as_of_time=now,
                )
                result["processed"] = [
                    {
                        "binding_id": item.binding_id,
                        "strategy_id": item.strategy_id,
                        "broker_backend": item.broker_backend.value,
                        "status": item.status,
                        "run_id": item.run.run_id if item.run else None,
                        "execution_plan_id": item.execution_plan.plan_id if item.execution_plan else None,
                        "data_source": item.data_source or self._data_source,
                        "error": item.error,
                    }
                    for item in tick.results
                ]
                result["summary"] = {
                    "total_bindings": tick.total_bindings,
                    "planned_count": tick.planned_count,
                    "reused_count": tick.reused_count,
                    "submitted_count": tick.submitted_count,
                    "failed_count": tick.failed_count,
                }
            except Exception as exc:  # scheduler must expose failure, not crash silently
                payload = {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "context": getattr(exc, "context", None),
                }
                result["errors"].append(payload)
                logger.warning("Simulation runtime scheduler tick failed: %s", payload)
        result["completed_at"] = datetime.now().isoformat()
        self._last_run_at = now
        self._last_result = result
        return result

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
        should_run = action in {"selection_evidence", "execution_plan", "submit"}
        submit = bool(self._default_submit and action == "submit")
        return {"window": active, "should_run": should_run, "submit": submit, "reason": action}

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
            return 30
        return value if value > 0 else 30

    @staticmethod
    def _default_limit() -> int:
        raw = (os.getenv("SIMULATION_RUNTIME_SCHEDULER_LIMIT") or "100").strip()
        try:
            value = int(raw)
        except ValueError:
            return 100
        return min(max(value, 1), 500)

    def _data_source_policy(self) -> dict[str, str]:
        return {
            "default": self._data_source,
            "local_sim_same_day": MinuteDataSource.TDX_REALTIME.value,
            "local_sim_historical": "persisted_portfolio_data_source",
            "miniqmt_sim": MinuteDataSource.MINIQMT_REALTIME.value,
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
